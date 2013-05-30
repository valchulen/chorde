# -*- coding: utf-8 -*-
from .inproc import InprocCacheClient # before base, really, it injects CacheMissError
from .base import BaseCacheClient, ReadWriteSyncAdapter, NONE, CacheMissError
from .async import AsyncWriteCacheClient, Deferk, AsyncCacheProcessor
from .tiered import TieredInclusiveClient
from .coherent import CoherentDefer, CoherentWrapperClient

__all__ = [
    "NONE",
    "CacheMissError",
    "CoherentDefer",
    "Defer",
    
    "AsyncWriteCacheClient",
    "BaseCacheClient",
    "CoherentWrapperClient",
    "InprocCacheClient",
    "ReadWriteSyncAdapter",
    "TieredInclusiveClient",
]

# Optionals below
try:
    from .memcached import MemcachedClient, FastMemcachedClient
    __all__.extend(["MemcachedClient", "FastMemcachedClient"])
except ImportError:
    pass
