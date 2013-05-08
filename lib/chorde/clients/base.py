# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod, abstractproperty
from serialize import serialize_read, serialize_write

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
    def get(self, key, default = NONE):
        if default is NONE:
            raise CacheMissError, key
        else:
            return default

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
    def contains(self, key):
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
    def get(self, key, default = NONE):
        return self.client.get(key, default)

    @serialize_write
    def clear(self):
        return self.client.clear()

    @serialize_write
    def purge(self, timeout = 0):
        return self.client.purge(timeout)

    @serialize_read
    def contains(self, key):
        return self.client.contains(key)
