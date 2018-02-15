# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest

from chorde import pylrucache

try:
    from chorde import lrucache
    skipIfNoLRUCache = lambda c : c
except ImportError:
    from chorde import pylrucache as lrucache  # lint:ok
    skipIfNoLRUCache = unittest.skip("Optimized LRUCache not built in")

class PyLRUCacheTest(unittest.TestCase):
    TEST_ELEMENTS = zip(range(10), range(10,20))
    Cache = pylrucache.LRUCache

    def testAdd(self, **kwargs):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = self.Cache(20, eviction_callback = eviction_callback, **kwargs)
        for k,v in self.TEST_ELEMENTS:
            c[k] = v

        self.assertEqual(len(evictions) + len(c), len(self.TEST_ELEMENTS))
        overflow = dict(evictions)
        for k,v in self.TEST_ELEMENTS:
            self.assertEqual(c.get(k, overflow.get(k)), v)

@skipIfNoLRUCache
class LRUCacheTest(PyLRUCacheTest):
    Cache = lrucache.LRUCache

    def testPreallocate(self):
        self.testAdd(preallocate = True)

