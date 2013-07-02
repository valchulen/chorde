# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod, abstractproperty
import operator
from chorde.serialize import serialize_read, serialize_write, serialize

# Overridden by inproc_cache based on LRUCache availability
CacheMissError = KeyError

class NONE: pass

class BaseCacheClient(object):
    """
    Interface of all backing stores.
    """
    __metaclass__ = ABCMeta

    @abstractproperty
    def async(self):
        return False

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
    def delete(self, key):
        raise NotImplementedError

    @abstractmethod
    def getTtl(self, key, default = NONE):
        """
        Returns: a tuple (value, ttl). If a default is given and the value
            is not in cache, return (default, -1). If a default is not given
            and the value is not in cache, raises CacheMissError. If the value
            is in the cache, but stale, ttl will be < 0, and value will be
            other than NONE. Note that ttl=0 is a valid and non-stale result.
        """
        if default is NONE:
            raise CacheMissError, key
        else:
            return (default, -1)
    
    def get(self, key, default = NONE):
        rv, ttl = self.getTtl(key, default)
        if ttl < 0 and default is NONE:
            raise CacheMissError, key
        else:
            return rv

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

    @serialize_write
    def put(self, key, value, ttl):
        return self.client.put(key, value, ttl)

    @serialize_write
    def delete(self, key):
        return self.client.delete(key)

    @serialize_read
    def getTtl(self, key, default = NONE):
        return self.client.getTtl(key, default)

    @serialize_write
    def clear(self):
        return self.client.clear()

    @serialize_write
    def purge(self, timeout = 0):
        return self.client.purge(timeout)

    @serialize_read
    def contains(self, key, ttl = None):
        return self.client.contains(key, ttl)


class SyncAdapter(BaseCacheClient):
    def __init__(self, client):
        self.client = client

    @property
    def async(self):
        return self.client.async

    @serialize
    def put(self, key, value, ttl):
        return self.client.put(key, value, ttl)

    @serialize
    def delete(self, key):
        return self.client.delete(key)

    @serialize
    def getTtl(self, key, default = NONE):
        return self.client.getTtl(key, default)

    @serialize
    def clear(self):
        return self.client.clear()

    @serialize
    def purge(self, timeout = 0):
        return self.client.purge(timeout)

    @serialize
    def contains(self, key, ttl = None):
        return self.client.contains(key, ttl)


class DecoratedWrapper(BaseCacheClient):
    """
    A namespace wrapper client will decorate keys with a namespace, making it possible
    to share one client among many sub-clients without key collisions.
    """
    def __init__(self, client, key_decorator = None, value_decorator = None, value_undecorator = None):
        self.client = client
        self.key_decorator = key_decorator
        self.value_decorator = value_decorator
        self.value_undecorator = value_undecorator

    @property
    def async(self):
        return self.client.async

    def wait(self, key, timeout = None):
        if self.key_decorator:
            key = self.key_decorator(key)
        return self.client.wait(key, timeout)
    
    def put(self, key, value, ttl):
        if self.key_decorator:
            key = self.key_decorator(key)
        if self.value_decorator:
            value = self.value_decorator(value)
        return self.client.put(key, value, ttl)

    def delete(self, key):
        if self.key_decorator:
            key = self.key_decorator(key)
        return self.client.delete(key)

    def getTtl(self, key, default = NONE):
        key_decorator = self.key_decorator
        if key_decorator is not None:
            key = key_decorator(key)
        rv = self.client.getTtl(key, default)
        if rv is not default and self.value_undecorator is not None:
            rv = self.value_undecorator(rv)
        return rv

    def clear(self):
        return self.client.clear()

    def purge(self, timeout = 0):
        return self.client.purge(timeout)

    def contains(self, key, ttl = None):
        key_decorator = self.key_decorator
        if key_decorator is not None:
            key = key_decorator(key)
        return self.client.contains(key, ttl)

class NamespaceWrapper(DecoratedWrapper):
    """
    A namespace wrapper client will decorate keys with a namespace, making it possible
    to share one client among many sub-clients without key collisions.
    """
    def __init__(self, namespace, client):
        super(NamespaceWrapper, self).__init__(client)
        self.namespace = namespace
        self.revision = client.get((namespace,'REVMARK'), 0)

    key_decorator = property(
        operator.attrgetter('_key_decorator'),
        lambda self, value : None)

    def _key_decorator(self, key):
        return (self.namespace, self.revision, key)

    def clear(self):
        # Cannot clear a shared client, so, instead, switch revisions
        self.revision += 1
        self.client.put((self.namespace, 'REVMARK'), self.revision, 3600)
        return self.client.clear()

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
        pass

    @property
    def namespace(self):
        return self.reference.namespace

    @namespace.setter
    def namespace(self):
        pass
