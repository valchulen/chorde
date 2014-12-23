# -*- coding: utf-8 -*-
from .inproc import InprocCacheClient # before base, really, it injects CacheMissError
from .base import BaseCacheClient, ReadWriteSyncAdapter, NONE, CacheMissError, TimeoutError, CancelledError
from .async import AsyncWriteCacheClient, Defer, AsyncCacheProcessor
from .tiered import TieredInclusiveClient

__all__ = [
    "NONE",
    "CacheMissError",
    "TimeoutError",
    "CancelledError",
    "Defer",
    
    "AsyncCacheProcessor",
    "AsyncWriteCacheClient",
    "BaseCacheClient",
    "InprocCacheClient",
    "ReadWriteSyncAdapter",
    "TieredInclusiveClient",
]

# Optionals below
try:
    from .memcached import MemcachedClient, FastMemcachedClient  # lint:ok
    __all__.extend(["MemcachedClient", "FastMemcachedClient"])
except ImportError:
    pass

try:
    from .coherent import CoherentDefer, CoherentWrapperClient  # lint:ok
    __all__.extend(["CoherentWrapperClient", "CoherentDefer"])
except ImportError:
    pass

try:
    from .files import FilesCacheClient  # lint:ok
    __all__.extend(["FilesCacheClient"])
except:
    pass

