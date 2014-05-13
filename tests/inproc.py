# -*- coding: utf-8 -*-
import unittest

from .clientbase import (CacheClientTestMixIn, 
    SyncWrapperTestMixIn, RWSyncWrapperTestMixIn,
    NamespaceWrapperTestMixIn
)

class InprocTest(CacheClientTestMixIn, unittest.TestCase):
    def setUpClient(self):
        from chorde.clients.inproc import InprocCacheClient
        return InprocCacheClient(100)

class PyInprocTest(CacheClientTestMixIn, unittest.TestCase):
    def setUpClient(self):
        from chorde.clients.inproc import InprocCacheClient
        from chorde import pylrucache
        return InprocCacheClient(100, store_class = pylrucache.LRUCache)

class SyncInprocTest(SyncWrapperTestMixIn, InprocTest):
    pass

class RWSyncInprocTest(RWSyncWrapperTestMixIn, InprocTest):
    pass

class NamespaceInprocTest(NamespaceWrapperTestMixIn, InprocTest):
    pass

