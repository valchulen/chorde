# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod, abstractproperty
import operator
from chorde.serialize import serialize_read, serialize_write, serialize

# Overridden by inproc_cache based on LRUCache availability
CacheMissError = KeyError

try:
    from concurrent.futures import TimeoutError, CancelledError
except ImportError:
    class TimeoutError(Exception):  # lint:ok
        pass
    class CancelledError(Exception):  # lint:ok
        pass

class NONE: pass
# 100 years ttl
FOREVER =  3600 * 24 * 30 * 12 * 100

class BaseCacheClient(object):
    """
    Interface of all backing stores.
    """
    __metaclass__ = ABCMeta

    @abstractproperty
    def async(self):
        return False

    @abstractproperty
    def capacity(self):
        raise NotImplementedError

    @abstractproperty
    def usage(self):
        raise NotImplementedError

    def wait(self, key, timeout = None):
        """
        Only valid for async clients, if there is a write pending, this
        will wait for it to finish. If there is not, it will return immediately.
        """
        pass

    @abstractmethod
    def put(self, key, value, ttl):
        raise NotImplementedError

    @abstractmethod
    def renew(self, key, ttl):
        """
        If there is a value for that key in the cache, and if its TTL is lower
        than the specified value, it will change its TTL in an atomic fashion
        and extend the life of that value.
        """
        raise NotImplementedError

    def add(self, key, value, ttl):
        """
        Like put, sets a value, but add only does so if there wasn't a valid
        value before, and does so as atomically as possible (ie: it tries to
        be atomic, but doesn't guarantee it - see specific client docs).

        Returns True when it effectively stores the item, False when it
        doesn't (there was one before)
        """
        if not self.contains(key):
            self.put(key, value, ttl)
            return True
        else:
            return False

    @abstractmethod
    def delete(self, key):
        raise NotImplementedError

    def expire(self, key):
        """
        If they given key is in the cache, it will set its TTL to 0, so
        getTtl will subsequently return either a miss, or an expired item.
        """
        return self.delete(key)

    @abstractmethod
    def getTtl(self, key, default = NONE, ttl_skip = None, promote_callback = None):
        """
        Returns: a tuple (value, ttl). If a default is given and the value
            is not in cache, return (default, -1). If a default is not given
            and the value is not in cache, raises CacheMissError. If the value
            is in the cache, but stale, ttl will be < 0, and value will be
            other than NONE. Note that ttl=0 is a valid and non-stale result.
            If ttl_skip is given, and the cache is an aggregate of multiple
            caches, entries with ttl below ttl_skip will be ignored, and
            automatic promotion of remaining entries will occur.

            If promote_callback is given, and the operation results in a promotion
            from a lower level to a higher one, promote_callback will be called
            with 3 arguments: the key as given, the value being promoted,
            and the remaining TTL.
        """
        if default is NONE:
            raise CacheMissError, key
        else:
            return (default, -1)

    def promote(self, key, default = NONE, ttl_skip = None, promote_callback = None):
        """
        Returns: None. If getTtl would find an entry with the given arguments,
            this entry will be promoted. Otherwise, it's a no-op.
        """
        return

    def get(self, key, default = NONE, **kw):
        rv, ttl = self.getTtl(key, default, **kw)
        if ttl < 0:
            if default is NONE:
                raise CacheMissError, key
            else:
                return default
        else:
            return rv

    def getMulti(self, keys, default = NONE, **kw):
        """
        Params:

            keys: An iterable of keys to fetch

            default: The default value to return when a miss occurs

            ttl_skip: (kwarg only) Clients that support it, will
                avoid returning entries with ttls below this

        Returns: An iterator over the respective (key, value) pairs. When a
            miss occurs, the default is returned in every case (instead of
            an exception). It's up to the caller to check for a default
            return value. The order of iteration may not match the given
            iterator.
        """
        ttl_skip = kw.get('ttl_skip') or 0
        for key, (rv, ttl) in self.getTtlMulti(keys, default, **kw):
            if ttl < ttl_skip:
                yield key, default
            else:
                yield key, rv

    def getTtlMulti(self, keys, default = NONE, **kw):
        """
        Params:

            keys: An iterable of keys to fetch

            default: The default value to return when a miss occurs

            ttl_skip: (kwarg only) Clients that support it, will
                avoid returning entries with ttls below this

        Returns: An iterator over the respective (key, (value, ttl)) tuples.
            When a miss occurs, the default is returned in every case (instead
            of an exception). It's up to the caller to check for a default
            return value. The order of iteration may not match the given
            iterator.
        """
        for key in keys:
            try:
                yield key, self.getTtl(key, default, **kw)
            except CacheMissError:
                yield key, (default, -1)

    @abstractmethod
    def clear(self):
        raise NotImplementedError

    @abstractmethod
    def purge(self, timeout = 0):
        """
        Params

            timeout: if specified, only items that have been stale for
                this amount of time will be removed. That is, it is added to the
                initial entry's TTL vale.
        """
        raise NotImplementedError

    @abstractmethod
    def contains(self, key, ttl = None):
        """
        Verifies that a key is valid within the cache

        Params

            key: the key to check

            ttl: If provided and not None, a TTL margin. Keys with this or less
                time to live will be considered as stale. Provide if you want
                to check about-to-expire keys.
        """
        return False

class ReadWriteSyncAdapter(BaseCacheClient):
    def __init__(self, client):
        self.client = client

    @property
    def async(self):
        return self.client.async

    @property
    def capacity(self):
        return self.client.capacity

    @property
    def usage(self):
        return self.client.usage

    @serialize_write
    def put(self, key, value, ttl, **kw):
        return self.client.put(key, value, ttl, **kw)

    @serialize_write
    def renew(self, key, ttl, **kw):
        return self.client.renew(key, ttl, **kw)

    @serialize_write
    def add(self, key, value, ttl, **kw):
        return self.client.add(key, value, ttl, **kw)

    @serialize_write
    def delete(self, key):
        return self.client.delete(key)

    @serialize_write
    def expire(self, key):
        return self.client.expire(key)

    @serialize_read
    def getTtl(self, key, default = NONE, **kw):
        return self.client.getTtl(key, default, **kw)

    @serialize_read
    def get(self, key, default = NONE, **kw):
        return self.client.get(key, default, **kw)

    @serialize_read
    def getTtlMulti(self, keys, default = NONE, **kw):
        return self.client.getTtlMulti(keys, default, **kw)

    @serialize_read
    def getMulti(self, keys, default = NONE, **kw):
        return self.client.getMulti(keys, default, **kw)

    @serialize_write
    def promote(self, *p, **kw):
        return self.client.promote(*p, **kw)

    @serialize_write
    def clear(self):
        return self.client.clear()

    @serialize_write
    def purge(self, *p, **kw):
        return self.client.purge(*p, **kw)

    @serialize_read
    def contains(self, key, ttl = None, **kw):
        return self.client.contains(key, ttl, **kw)

    def __str__(self):
        return "<%s of %r>" % (self.__class__.__name__, self.client)

    def __repr__(self):
        return str(self)


class SyncAdapter(BaseCacheClient):
    def __init__(self, client):
        self.client = client

    @property
    def async(self):
        return self.client.async

    @property
    def capacity(self):
        return self.client.capacity

    @property
    def usage(self):
        return self.client.usage

    @serialize
    def put(self, key, value, ttl, **kw):
        return self.client.put(key, value, ttl, **kw)

    @serialize
    def renew(self, key, ttl, **kw):
        return self.client.renew(key, ttl, **kw)

    @serialize
    def add(self, key, value, ttl, **kw):
        return self.client.add(key, value, ttl, **kw)

    @serialize
    def delete(self, key):
        return self.client.delete(key)

    @serialize
    def expire(self, key):
        return self.client.expire(key)

    @serialize
    def getTtl(self, key, default = NONE, **kw):
        return self.client.getTtl(key, default, **kw)

    @serialize
    def get(self, key, default = NONE, **kw):
        return self.client.get(key, default, **kw)

    @serialize
    def getTtlMulti(self, keys, default = NONE, **kw):
        return self.client.getTtlMulti(keys, default, **kw)

    @serialize
    def getMulti(self, keys, default = NONE, **kw):
        return self.client.getMulti(keys, default, **kw)

    @serialize_write
    def promote(self, *p, **kw):
        return self.client.promote(*p, **kw)

    @serialize
    def clear(self):
        return self.client.clear()

    @serialize
    def purge(self, *p, **kw):
        return self.client.purge(*p, **kw)

    @serialize
    def contains(self, key, ttl = None, **kw):
        return self.client.contains(key, ttl, **kw)

    def __str__(self):
        return "<%s of %r>" % (self.__class__.__name__, self.client)

    def __repr__(self):
        return str(self)


class DecoratedWrapper(BaseCacheClient):
    """
    A namespace wrapper client will decorate keys with a namespace, making it possible
    to share one client among many sub-clients without key collisions.
    """
    def __init__(self, client,
            key_decorator = None, key_undecorator = None,
            value_decorator = None, value_undecorator = None):
        self.client = client
        self.key_decorator = key_decorator
        self.key_undecorator = key_undecorator
        self.value_decorator = value_decorator
        self.value_undecorator = value_undecorator

    @property
    def async(self):
        return self.client.async

    @property
    def capacity(self):
        return self.client.capacity

    @property
    def usage(self):
        return self.client.usage

    def wait(self, key, timeout = None):
        key_decorator = self.key_decorator
        if key_decorator:
            key = key_decorator(key)
        return self.client.wait(key, timeout)

    def put(self, key, value, ttl, **kw):
        key_decorator = self.key_decorator
        if key_decorator:
            key = key_decorator(key)
        value_decorator = self.value_decorator
        if value_decorator:
            if hasattr(value, 'undefer'):
                callable_ = value.callable_
                value.callable_ = lambda *p, **kw: value_decorator(callable_(*p, **kw))
            else:
                value = value_decorator(value)
        return self.client.put(key, value, ttl, **kw)

    def put_coherently(self, key, ttl, expired, future, callable_, **kw):
        key_decorator = self.key_decorator
        if key_decorator:
            key = key_decorator(key)
        value_decorator = self.value_decorator
        if value_decorator:
            callable_ = lambda *p, **kw: value_decorator(callable_(*p, **kw))
        return self.client.put_coherently(key, ttl, expired, future, callable_, **kw)

    def renew(self, key, ttl, **kw):
        key_decorator = self.key_decorator
        if key_decorator:
            key = key_decorator(key)
        return self.client.renew(key, ttl, **kw)

    def add(self, key, value, ttl, **kw):
        key_decorator = self.key_decorator
        if key_decorator:
            key = key_decorator(key)
        value_decorator = self.value_decorator
        if value_decorator:
            value = value_decorator(value)
        return self.client.add(key, value, ttl, **kw)

    def delete(self, key):
        key_decorator = self.key_decorator
        if key_decorator:
            key = key_decorator(key)
        return self.client.delete(key)

    def expire(self, key):
        key_decorator = self.key_decorator
        if key_decorator:
            key = key_decorator(key)
        return self.client.expire(key)

    def _wrapPromoteCallback(self, key_undecorator, kw):
        promote_callback = kw['promote_callback']
        def undecorating_callback(key, value, ttl):
            promote_callback(key_undecorator(key), value, ttl)
        kw['promote_callback'] = undecorating_callback

    def getTtl(self, key, default = NONE, **kw):
        key_decorator = self.key_decorator
        if key_decorator is not None:
            key = key_decorator(key)
            key_undecorator = self.key_undecorator
            if key_undecorator is not None and 'promote_callback' in kw:
                self._wrapPromoteCallback(key_undecorator, kw)
        rv, ttl = self.client.getTtl(key, default, **kw)
        if rv is not default:
            value_undecorator = self.value_undecorator
            if value_undecorator is not None:
                rv = value_undecorator(rv)
        return rv, ttl

    def get(self, key, default = NONE, **kw):
        key_decorator = self.key_decorator
        if key_decorator is not None:
            key = key_decorator(key)
            key_undecorator = self.key_undecorator
            if key_undecorator is not None and 'promote_callback' in kw:
                self._wrapPromoteCallback(key_undecorator, kw)
        rv = self.client.get(key, default, **kw)
        if rv is not default:
            value_undecorator = self.value_undecorator
            if value_undecorator is not None:
                rv = value_undecorator(rv)
        return rv

    def getTtlMulti(self, keys, default = NONE, **kw):
        key_decorator = self.key_decorator
        key_undecorator = self.key_undecorator
        if key_decorator is not None:
            if key_undecorator is not None and 'promote_callback' in kw:
                self._wrapPromoteCallback(key_undecorator, kw)
            keys = map(key_decorator, keys)
        value_undecorator = self.value_undecorator
        for gkey, (rv, ttl) in self.client.getTtlMulti(keys, default, **kw):
            if rv is not default and value_undecorator is not None:
                rv = value_undecorator(rv)
            if key_undecorator is not None:
                gkey = key_undecorator(gkey)
            yield gkey, (rv, ttl)

    def getMulti(self, keys, default = NONE, **kw):
        key_decorator = self.key_decorator
        key_undecorator = self.key_undecorator
        if key_decorator is not None:
            if key_undecorator is not None and 'promote_callback' in kw:
                self._wrapPromoteCallback(key_undecorator, kw)
            keys = map(key_decorator, keys)
        value_undecorator = self.value_undecorator
        for gkey, rv in self.client.getMulti(keys, default, **kw):
            if rv is not default and value_undecorator is not None:
                rv = value_undecorator(rv)
            if key_undecorator is not None:
                gkey = key_undecorator(gkey)
            yield gkey, rv

    def promote(self, key, *p, **kw):
        key_decorator = self.key_decorator
        if key_decorator is not None:
            key = key_decorator(key)
            key_undecorator = self.key_undecorator
            if key_undecorator is not None and 'promote_callback' in kw:
                self._wrapPromoteCallback(key_undecorator, kw)
        self.client.promote(key, *p, **kw)

    def clear(self):
        return self.client.clear()

    def purge(self, *p, **kw):
        return self.client.purge(*p, **kw)

    def contains(self, key, ttl = None, **kw):
        key_decorator = self.key_decorator
        if key_decorator is not None:
            key = key_decorator(key)
        return self.client.contains(key, ttl, **kw)

class NamespaceWrapper(DecoratedWrapper):
    """
    A namespace wrapper client will decorate keys with a namespace, making it possible
    to share one client among many sub-clients without key collisions.
    """
    def __init__(self, namespace, client, revmark_ttl=FOREVER):
        super(NamespaceWrapper, self).__init__(client)
        self.namespace = namespace
        self.revision = client.get((namespace,'REVMARK'), 0)
        self.key_undecorator = operator.itemgetter(2)
        self.revmark_ttl = revmark_ttl

    key_decorator = property(
        operator.attrgetter('_key_decorator'),
        lambda self, value : None)

    def _key_decorator(self, key):
        return (self.namespace, self.revision, key)

    def getTtl(self, key, default = NONE, **kw):
        return self.client.getTtl(self._key_decorator(key), default, **kw)

    def get(self, key, default = NONE, **kw):
        return self.client.get(self._key_decorator(key), default, **kw)

    def getTtlMulti(self, keys, default = NONE, **kw):
        # Specialized implementation of DecoratedWrapper
        key_decorator = self._key_decorator
        key_undecorator = self.key_undecorator
        if key_decorator is not None:
            if 'promote_callback' in kw:
                self._wrapPromoteCallback(key_undecorator, kw)
            keys = map(key_decorator, keys)
        for gkey, (rv, ttl) in self.client.getTtlMulti(keys, default, **kw):
            yield key_undecorator(gkey), (rv, ttl)

    def getMulti(self, keys, default = NONE, **kw):
        # Specialized implementation of DecoratedWrapper
        key_decorator = self._key_decorator
        key_undecorator = self.key_undecorator
        if key_decorator is not None:
            if 'promote_callback' in kw:
                self._wrapPromoteCallback(key_undecorator, kw)
            keys = map(key_decorator, keys)
        for gkey, rv in self.client.getMulti(keys, default, **kw):
            yield key_undecorator(gkey), rv

    def contains(self, key, ttl = None, **kw):
        return self.client.contains(self._key_decorator(key), ttl, **kw)

    def clear(self):
        # Cannot clear a shared client, so, instead, switch revisions
        self.revision += 1
        self.client.put((self.namespace, 'REVMARK'), self.revision, self.revmark_ttl)

    def __str__(self):
        return "<%s namespace %r/%r on %r>" % (self.__class__.__name__, self.namespace, self.revision, self.client)

    def __repr__(self):
        return str(self)

class NamespaceMirrorWrapper(NamespaceWrapper):
    """
    A namespace wrapper that takes its namespace info from the provided reference (mirrors it).
    It also takes the internal revision number, to inherit namespace changes created by cache clears.
    """
    def __init__(self, reference, client):
        super(NamespaceWrapper, self).__init__(client)
        self.reference = reference

    @property
    def revision(self):
        return self.reference.revision

    @revision.setter
    def revision(self, value):
        self.reference.revision = value

    @property
    def namespace(self):
        return self.reference.namespace

    @namespace.setter
    def namespace(self, value):  # lint:ok
        pass

    @property
    def key_undecorator(self):
        return self.reference.key_undecorator

    @key_undecorator.setter
    def key_undecorator(self, value):  # lint:ok
        pass

    @property
    def key_decorator(self):
        return self.reference.key_decorator

    @key_decorator.setter
    def key_decorator(self, value):  # lint:ok
        pass

    @property
    def revmark_ttl(self):
        return self.reference.revmark_ttl

    @revmark_ttl.setter
    def revmark_ttl(self, value):
        self.reference.revmark_ttl = value
