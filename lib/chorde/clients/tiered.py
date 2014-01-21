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
    
    def __putnext(self, clients, fractions, key, value, ttl):
        deferred = value
        try:
            value = value.undefer()
            if value is not NONE and value is not async._NONE:
                for fraction, client in islice(izip(fractions,clients), 1, None):
                    try:
                        client.put(key, value, ttl * fraction)
                    except:
                        logging.getLogger('chorde').error("Error propagating deferred value through tier %r", client)
            return value
        finally:
            deferred.done()
    
    def put(self, key, value, ttl):
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
                    key, value, ttl)
                clients[0].put(key, deferred, ttl * fractions[0])
            else:
                # Cannot undefer here, it might create deadlocks.
                # Raise error.
                raise ValueError, "Sync first tier, cannot undefer"
        else:
            # Simple case
            for ttl_fraction, client in izip(fractions, clients):
                client.put(key, value, ttl * ttl_fraction)
    
    def add(self, key, value, ttl):
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
                    key, value, ttl)
                return clients[0].add(key, deferred, ttl * fractions[0])
            else:
                # Cannot undefer here, it might create deadlocks.
                # Raise error.
                raise ValueError, "Sync first tier, cannot undefer"
        else:
            # Simple case
            for ttl_fraction, client in izip(fractions, clients):
                if not client.add(key, value, ttl * ttl_fraction):
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
    
    def _getTtl(self, key, default = NONE, _max_tiers = None, ttl_skip = 0, **kw):
        ttl = -1
        NONE__ = NONE_
        clients = self.clients
        if _max_tiers is not None:
            clients = islice(clients, _max_tiers)
        for i,client in enumerate(clients):
            # Yeap, separate NONE_, we must avoid CacheMissError s
            rv, ttl = client.getTtl(key, NONE__)
            if rv is not NONE__ and ttl >= ttl_skip:
                # Cool
                if i > 0 and ttl > ttl_skip:
                    # Um... not first-tier
                    # Move the entry up the ladder
                    for i in xrange(i-1, -1, -1):
                        try:
                            self.clients[i].put(key, rv, ttl)
                        except:
                            # Ignore, go to the next
                            logging.getLogger('chorde').error("Error promoting into tier %d", i+1, exc_info = True)
                return rv, ttl
            elif not i:
                self.l1_misses += 1
            
            # Ok, gotta inspect other tiers
        else:
            # Or not
            if rv is not NONE__:
                return rv, ttl
            else:
                if default is NONE:
                    raise CacheMissError, key
                else:
                    return default, -1

    def getTtl(self, *p, **kw):
        return self._getTtl(*p, **kw)
    
    def promote(self, key, default = NONE, _max_tiers = None, ttl_skip = 0, **kw):
        ttl = -1
        NONE__ = NONE_
        clients = self.clients
        ttl_skip = ttl_skip or 0
        if _max_tiers is not None:
            clients = islice(clients, _max_tiers)
        for i,client in enumerate(clients):
            # Yeap, separate NONE_, we must avoid CacheMissError s
            if client.contains(key, ttl_skip):
                rv, ttl = client.getTtl(key, NONE__)
                if rv is not NONE__ and ttl > ttl_skip and i > 0:
                    for i in xrange(i-1, -1, -1):
                        try:
                            self.clients[i].put(key, rv, ttl)
                        except:
                            # Ignore, go to the next
                            logging.getLogger('chorde').error("Error promoting into tier %d", i+1, exc_info = True)
                    break
            # Ok, gotta inspect other tiers
    
    def contains(self, key, ttl = None, _max_tiers = None, **kw):
        clients = self.clients
        if _max_tiers is not None:
            clients = islice(clients, _max_tiers)
        for i,client in enumerate(clients):
            if client.contains(key, ttl):
                return True
            elif not i:
                self.l1_misses += 1
        else:
            return False

