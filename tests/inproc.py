# -*- coding: utf-8 -*-
import unittest

from .clientbase import (CacheClientTestMixIn,
    SyncWrapperTestMixIn, RWSyncWrapperTestMixIn,
    NamespaceWrapperTestMixIn
)
from clru.lrucache import pylrucache

from chorde.clients.base import NamespaceMirrorWrapper

class InprocTest(CacheClientTestMixIn, unittest.TestCase):
    def setUpClient(self):
        from chorde.clients.inproc import InprocCacheClient
        return InprocCacheClient(100)

class PyInprocTest(CacheClientTestMixIn, unittest.TestCase):
    def setUpClient(self):
        from chorde.clients.inproc import InprocCacheClient
        return InprocCacheClient(100, store_class = pylrucache.LRUCache)

class PyCuckooInprocTest(CacheClientTestMixIn, unittest.TestCase):
    is_lru = False

    def setUpClient(self):
        from chorde.clients.inproc import InprocCacheClient
        from clru.cuckoocache.pycuckoocache import LazyCuckooCache
        return InprocCacheClient(100, store_class = LazyCuckooCache)

try:
    from clru.cuckoocache import cycuckoocache as cuckoocache
    skipIfNotCythonized = lambda c : c
except ImportError:
    from clru.cuckoocache import pycuckoocache as cuckoocache # lint:ok
    skipIfNotCythonized = unittest.skip("Optimized LazyCuckooCache not built in")

@skipIfNotCythonized
class CuckooInprocTest(CacheClientTestMixIn, unittest.TestCase):
    is_lru = False

    def setUpClient(self):
        from chorde.clients.inproc import InprocCacheClient
        from clru.cuckoocache.cycuckoocache  import LazyCuckooCache
        return InprocCacheClient(100, store_class = LazyCuckooCache)

class SyncInprocTest(SyncWrapperTestMixIn, InprocTest):
    pass

class RWSyncInprocTest(RWSyncWrapperTestMixIn, InprocTest):
    pass

class NamespaceInprocTest(NamespaceWrapperTestMixIn, InprocTest):
    pass

class NamespaceMirrorInprocTest(NamespaceWrapperTestMixIn, InprocTest):
    def _setUpClient(self):
        c = super(NamespaceMirrorInprocTest, self)._setUpClient()
        return NamespaceMirrorWrapper(c, self.rclient)
