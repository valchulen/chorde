# -*- coding: utf-8 -*-
from .inproc import InprocCacheClient # before base, really, it injects CacheMissError
from .base import BaseCacheClient, ReadWriteSyncAdapter, NONE, CacheMissError
from .async import AsyncWriteCacheClient, Defer

__all__ = [
    "NONE",
    "CacheMissError",
    "Defer",
    
    "InprocCacheClient",
    "ReadWriteSyncAdapter",
    "BaseCacheClient",
    "AsyncWriteCacheClient",
]

# Optionals below
try:
    from .memcached import MemcachedClient
    __all__.append("MemcachedClient")
except ImportError:
    pass
