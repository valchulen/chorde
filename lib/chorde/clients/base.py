# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod, abstractproperty
from chorde.serialize import serialize_read, serialize_write

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
