# -*- coding: utf-8 -*-
import async
from .base import NONE, CacheMissError, BaseCacheClient

class NONE_: pass

class TieredInclusiveClient(BaseCacheClient):
    def __init__(self, *clients):
        self.clients = clients
        
    @property
    def async(self):
        for client in self.clients:
            if client.async:
                return True
        else:
            return False
    
    def __putnext(self, clients, key, value, ttl):
        value = value.undefer()
        for client in clients[1:]:
            client.put(key, value, ttl)
        return value
    
    def put(self, key, value, ttl):
        if isinstance(value, async.Defer):
            # Cannot just pass it, it would execute the result many times
            if self.clients and self.clients[0].async:
                # First call is async, meaning it will get queued up somwhere
                # We can do the rest at that point
                deferred = async.Defer(self.__putnext, key, value, ttl)
                self.clients[0].put(key, deferred, ttl)
            else:
                # First call is sync, meaning we must undefer, no choice
                value = value.undefer()
                for client in self.clients:
                    client.put(key, value, ttl)
        else:
            # Simple case
            for client in self.clients:
                client.put(key, value, ttl)
    
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
        for i,client in enumerate(self.clients):
            # Yeap, separate NONE_, we must avoid CacheMissError s
            rv, ttl = client.getTtl(key, NONE_)
            if rv is not NONE_ and ttl >= 0:
                # Cool
                if i > 0 and ttl > 0:
                    # Um... not first-tier
                    # Move the entry up the ladder
                    self.clients[i-1].put(key, rv, ttl)
                return rv, ttl
            
            # Ok, gotta inspect other tiers
        else:
            # Or not
            if rv is not NONE_:
                return rv, ttl
            else:
                if default is NONE:
                    raise CacheMissError, key
                else:
                    return default, -1
    
    def contains(self, key):
        for client in self.clients:
            if client.contains(key):
                return True
        else:
            return False

