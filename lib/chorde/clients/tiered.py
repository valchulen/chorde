# -*- coding: utf-8 -*-
from itertools import izip, islice
import operator
import logging

from . import async
from .base import NONE, CacheMissError, BaseCacheClient

class NONE_: pass

class TieredInclusiveClient(BaseCacheClient):
    def __init__(self, *clients, **opts):
        self.clients = clients
        self.l1_misses = 0
        self.ttl_fractions = opts.get('ttl_fractions', (1,)*len(clients))

    @property
    def async(self):
        for client in self.clients:
            if client.async:
                return True
        else:
            return False

    @property
    def capacity(self):
        return map(operator.attrgetter('capacity'), self.clients)

    @property
    def usage(self):
        return map(operator.attrgetter('usage'), self.clients)

    def wait(self, key, timeout = None):
        for client in self.clients:
            client.wait(key, timeout)

    def __putnext(self, clients, fractions, key, value, ttl, _max_tiers=None, **kw):
        deferred = value
        try:
            value = value.undefer()
            if value is async.REGET:
                # This will cause a CancelledError on any waiter
                # if we don't get a better value, which is what we want
                value = async._NONE

                # NONE_ is a special local value that does not raise CacheMissErrors
                reget_value, vttl = self.getTtl(key, NONE_, return_stale = False)
                if reget_value is not NONE_ and vttl > 0:
                    # This might be an old value, so try to promote better values from upper tiers
                    value = reget_value
                    reget_value, vttl = self.getTtl(key, NONE_, ttl_skip = vttl+1, return_stale = False)
                    if reget_value is not NONE_:
                        value = reget_value
                deferred.set(value)

                # In any case, don't do the reget in the caller, we did the equivalent
                value = async._NONE
            elif value is not NONE and value is not async._NONE:
                for fraction, client in islice(izip(fractions,clients), 1, _max_tiers):
                    try:
                        client.put(key, value, ttl * fraction, **kw)
                    except:
                        logging.getLogger('chorde.tiered').error("Error propagating deferred value through tier %r", client)
            return value
        finally:
            deferred.done()

    def put(self, key, value, ttl, _max_tiers=None, **kw):
        clients = self.clients
        fractions = self.ttl_fractions
        if isinstance(value, async.Defer):
            # Cannot just pass it, it would execute the result many times
            if clients and clients[0].async:
                # First call is async, meaning it will get queued up somwhere
                # We can do the rest at that point
                deferred = async.Defer(
                    self.__putnext,
                    clients, fractions,
                    key, value, ttl, _max_tiers, **kw)
                if hasattr(value, 'future'):
                    # Transfer the original deferred's future to this new one-shot deferred
                    deferred.future = value.future
                clients[0].put(key, deferred, ttl * fractions[0], **kw)
            else:
                # Cannot undefer here, it might create deadlocks.
                # Raise error.
                raise ValueError, "Sync first tier, cannot undefer"
        else:
            # Simple case
            tiers = izip(fractions, clients)
            if _max_tiers is not None:
                tiers = islice(tiers, _max_tiers)
            for ttl_fraction, client in tiers:
                client.put(key, value, ttl * ttl_fraction, **kw)

    def renew(self, key, ttl, _max_tiers=None, **kw):
        clients = self.clients
        fractions = self.ttl_fractions
        tiers = izip(fractions, clients)
        if _max_tiers is not None:
            tiers = islice(tiers, _max_tiers)
        for ttl_fraction, client in tiers:
            client.renew(key, ttl * ttl_fraction, **kw)

    def add(self, key, value, ttl, _max_tiers=None, **kw):
        clients = self.clients
        fractions = self.ttl_fractions
        if isinstance(value, async.Defer):
            # Cannot just pass it, it would execute the result many times
            if clients and clients[0].async:
                # First call is async, meaning it will get queued up somwhere
                # We can do the rest at that point
                deferred = async.Defer(
                    self.__putnext,
                    clients, fractions,
                    key, value, ttl, _max_tiers, **kw)
                return clients[0].add(key, deferred, ttl * fractions[0], **kw)
            else:
                # Cannot undefer here, it might create deadlocks.
                # Raise error.
                raise ValueError, "Sync first tier, cannot undefer"
        else:
            # Simple case
            tiers = izip(fractions, clients)
            if _max_tiers is not None:
                tiers = islice(tiers, _max_tiers)
            for ttl_fraction, client in tiers:
                if not client.add(key, value, ttl * ttl_fraction, **kw):
                    return False
            else:
                return True

    def delete(self, key):
        for client in self.clients:
            client.delete(key)

    def expire(self, key):
        for client in self.clients:
            client.expire(key)

    def clear(self):
        for client in self.clients:
            client.clear()

    def purge(self, *p, **kw):
        for client in self.clients:
            client.purge(*p, **kw)

    def _getTtl(self, key, default = NONE, _max_tiers = None, ttl_skip = 0,
            promote_callback = None, return_stale = True,
            NONE_ = NONE_, NONE = NONE, enumerate = enumerate, islice = islice,
            **kw):
        ttl = -1
        clients = self.clients
        if _max_tiers is not None:
            clients = islice(clients, _max_tiers)
        for i, client in enumerate(clients):
            # Yeap, separate NONE_, we must avoid CacheMissError s
            rv, ttl = client.getTtl(key, NONE_)
            if rv is not NONE_ and ttl >= ttl_skip:
                # Cool
                if i > 0 and ttl > ttl_skip:
                    # Um... not first-tier
                    # Move the entry up the ladder
                    for i in xrange(i-1, -1, -1):
                        try:
                            self.clients[i].put(key, rv, ttl)
                        except:
                            # Ignore, go to the next
                            logging.getLogger('chorde.tiered').error("Error promoting into tier %d", i+1, exc_info = True)
                    if promote_callback:
                        try:
                            promote_callback(key, rv, ttl)
                        except:
                            # Ignore
                            logging.getLogger('chorde.tiered').error("Error on promote callback", exc_info = True)
                return rv, ttl
            elif not i:
                self.l1_misses += 1

            # Ok, gotta inspect other tiers
        else:
            # Or not
            if rv is not NONE_ and (return_stale or ttl_skip is None or ttl >= ttl_skip):
                return rv, ttl
            else:
                if default is NONE:
                    raise CacheMissError, key
                else:
                    return default, -1

    getTtl = _getTtl

    def _getTtlMulti(self, keys, default = NONE, _max_tiers = None, ttl_skip = 0, promote_callback = None,
            NONE_ = NONE_, NONE = NONE, enumerate = enumerate, islice = islice,
            **kw):

        default_rv = (default, -1)
        stale = {}
        clients = self.clients
        if _max_tiers is not None:
            clients = islice(clients, _max_tiers)
        for i, client in enumerate(clients):
            # Yeap, separate NONE_, we must avoid CacheMissError s
            nkeys = []
            nkeys_append = nkeys.append
            mrv = client.getTtlMulti(keys, NONE_)
            for key, (rv, ttl) in mrv:
                if rv is not NONE_ and ttl >= ttl_skip:
                    # Cool
                    if i > 0 and ttl > ttl_skip:
                        # Um... not first-tier
                        # Move the entry up the ladder
                        for i in xrange(i-1, -1, -1):
                            try:
                                self.clients[i].put(key, rv, ttl)
                            except:
                                # Ignore, go to the next
                                logging.getLogger('chorde.tiered').error("Error promoting into tier %d", i+1, exc_info = True)
                        if promote_callback:
                            try:
                                promote_callback(key, rv, ttl)
                            except:
                                # Ignore
                                logging.getLogger('chorde.tiered').error("Error on promote callback", exc_info = True)
                    yield key, (rv, ttl)
                else:
                    if rv is not NONE_:
                        # remember stale response
                        stale[key] = (rv, ttl)
                    if not i:
                        self.l1_misses += 1
                    nkeys_append(key)

            keys = nkeys
            del nkeys, nkeys_append
            if not keys:
                break
            # Ok, gotta inspect other tiers
        else:
            for key in keys:
                yield key, stale.get(key, default_rv)

    getTtlMulti = _getTtlMulti

    def promote(self, key, default = NONE, _max_tiers = None, ttl_skip = 0, promote_callback = None, **kw):
        ttl = -1
        NONE__ = NONE_
        clients = self.clients
        ttl_skip = ttl_skip or 0
        if _max_tiers is not None:
            clients = islice(clients, _max_tiers)
        for i, client in enumerate(clients):
            # Yeap, separate NONE_, we must avoid CacheMissError s
            if client.contains(key, ttl_skip):
                rv, ttl = client.getTtl(key, NONE__)
                if rv is not NONE__ and ttl > ttl_skip and i > 0:
                    for i in xrange(i-1, -1, -1):
                        try:
                            self.clients[i].put(key, rv, ttl)
                        except:
                            # Ignore, go to the next
                            logging.getLogger('chorde.tiered').error("Error promoting into tier %d", i+1, exc_info = True)
                    if promote_callback:
                        try:
                            promote_callback(key, rv, ttl)
                        except:
                            # Ignore
                            logging.getLogger('chorde.tiered').error("Error on promote callback", exc_info = True)
                # Even if we don't really promote, stop trying
                # If the above client.contains returns True but getTtl doesn't find it,
                # it's probably an enqueued deferred write, which means we shouldn't promote anyway
                break
            # Ok, gotta inspect other tiers

    def contains(self, key, ttl = None, _max_tiers = None, **kw):
        clients = self.clients
        if _max_tiers is not None:
            clients = islice(clients, _max_tiers)
        for i, client in enumerate(clients):
            if client.contains(key, ttl):
                return True
            elif not i:
                self.l1_misses += 1
        else:
            return False

