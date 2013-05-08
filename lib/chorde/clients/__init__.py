# -*- coding: utf-8 -*-
from inproc_cache import InprocCacheClient

__all__ = [
    "InprocCacheClient",
]

try:
    from .memcached import MemcachedClient
    __all__.append("MemcachedClient")
except ImportError:
    pass
