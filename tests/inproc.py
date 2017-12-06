# -*- coding: utf-8 -*-
import unittest

from .clientbase import (CacheClientTestMixIn,
    SyncWrapperTestMixIn, RWSyncWrapperTestMixIn,
    NamespaceWrapperTestMixIn
)

from chorde.clients.base import NamespaceMirrorWrapper

class InprocTest(CacheClientTestMixIn, unittest.TestCase):
    def setUpClient(self):
        from chorde.clients.inproc import InprocCacheClient
        return InprocCacheClient(100)

class PyInprocTest(CacheClientTestMixIn, unittest.TestCase):
    def setUpClient(self):
        from chorde.clients.inproc import InprocCacheClient
        from chorde import pylrucache
        return InprocCacheClient(100, store_class = pylrucache.LRUCache)

class PyCuckooInprocTest(CacheClientTestMixIn, unittest.TestCase):
    is_lru = False

    def setUpClient(self):
        from chorde.clients.inproc import InprocCacheClient
        from chorde.pycuckoocache import LazyCuckooCache
        return InprocCacheClient(100, store_class = LazyCuckooCache)

try:
    from chorde import cuckoocache
    skipIfNotCythonized = lambda c : c
except ImportError:
    from chorde import pycuckoocache as cuckoocache # lint:ok
    skipIfNotCythonized = unittest.skip("Optimized LazyCuckooCache not built in")

@skipIfNotCythonized
class CuckooInprocTest(CacheClientTestMixIn, unittest.TestCase):
    is_lru = False

    def setUpClient(self):
        from chorde.clients.inproc import InprocCacheClient
        from chorde.cuckoocache import LazyCuckooCache
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
