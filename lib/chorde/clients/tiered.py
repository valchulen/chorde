# -*- coding: utf-8 -*-
from itertools import izip, islice
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

    def wait(self, key, timeout = None):
        for client in self.clients:
            client.wait(key, timeout)
    
    def __putnext(self, clients, fractions, key, value, ttl):
        deferred = value
        value = value.undefer()
        if value is not NONE and value is not async._NONE:
            for fraction, client in islice(izip(fractions,clients), 1, None):
                try:
                    client.put(key, value, ttl * fraction)
                except:
                    logging.error("Error propagating deferred value through tier %r", client)
        deferred.done()
        return value
    
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

    def clear(self):
        for client in self.clients:
            client.clear()

    def purge(self):
        for client in self.clients:
            client.purge()
    
    def getTtl(self, key, default = NONE):
        ttl = -1
        NONE__ = NONE_
        for i,client in enumerate(self.clients):
            # Yeap, separate NONE_, we must avoid CacheMissError s
            rv, ttl = client.getTtl(key, NONE__)
            if rv is not NONE__ and ttl >= 0:
                # Cool
                if i > 0 and ttl > 0:
                    # Um... not first-tier
                    # Move the entry up the ladder
                    self.clients[i-1].put(key, rv, ttl)
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
    
    def contains(self, key, ttl = None):
        for i,client in enumerate(self.clients):
            if client.contains(key, ttl):
                return True
            elif not i:
                self.l1_misses += 1
        else:
            return False

