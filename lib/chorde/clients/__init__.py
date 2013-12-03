# -*- coding: utf-8 -*-
from .inproc import InprocCacheClient # before base, really, it injects CacheMissError
from .base import BaseCacheClient, ReadWriteSyncAdapter, NONE, CacheMissError
from .async import AsyncWriteCacheClient, Defer, AsyncCacheProcessor
from .tiered import TieredInclusiveClient
from .files import FilesCacheClient

__all__ = [
    "NONE",
    "CacheMissError",
    "Defer",
    
    "AsyncCacheProcessor",
    "AsyncWriteCacheClient",
    "BaseCacheClient",
    "InprocCacheClient",
    "ReadWriteSyncAdapter",
    "TieredInclusiveClient",
    "FilesCacheClient",
]

# Optionals below
try:
    from .memcached import MemcachedClient, FastMemcachedClient
    __all__.extend(["MemcachedClient", "FastMemcachedClient"])
except ImportError:
    pass

try:
    from .coherent import CoherentDefer, CoherentWrapperClient
    __all__.extend(["CoherentWrapperClient", "CoherentDefer"])
except ImportError:
    pass
