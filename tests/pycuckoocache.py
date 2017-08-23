# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest

from chorde import pycuckoocache

try:
    from chorde import cuckoocache
    skipIfNotCythonized = lambda c : c
except ImportError:
    from chorde import pycuckoocache as cuckoocache # lint:ok
    skipIfNotCythonized = unittest.skip("Optimized LazyCuckooCache not built in")

class PyCuckooCacheTest(unittest.TestCase):
    TEST_ELEMENTS = zip(range(10), range(10,20))
    Cache = pycuckoocache.LazyCuckooCache

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

    def testNoCallback(self):
        c = self.Cache(20)
        for k,v in self.TEST_ELEMENTS:
            c[k] = v

        self.assertLessEqual(len(c), len(self.TEST_ELEMENTS))
        for k,v in self.TEST_ELEMENTS:
            if k in c:
                self.assertEqual(c.get(k), v)
                self.assertEqual(c[k], v)
                self.assertIs(c.setdefault(k, None), v)

    def testContains(self, **kwargs):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = self.Cache(20, eviction_callback = eviction_callback, **kwargs)

        for k,v in self.TEST_ELEMENTS:
            self.assertFalse(k in c)

        for k,v in self.TEST_ELEMENTS:
            c[k] = v

        self.assertEqual(len(evictions) + len(c), len(self.TEST_ELEMENTS))
        overflow = dict(evictions)
        for k,v in self.TEST_ELEMENTS:
            self.assertTrue(k in c or k in overflow)

    def testSetDefault(self, **kwargs):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = self.Cache(20, eviction_callback = eviction_callback, **kwargs)

        for k,v in self.TEST_ELEMENTS:
            # Construct a distinct object
            v = float(v)
            self.assertIs(c.setdefault(k,v), v)

            # Second time returns the first one
            v2 = float(v)
            self.assertIs(c.setdefault(k,v2), v)

    def testCas(self, **kwargs):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = self.Cache(20, eviction_callback = eviction_callback, **kwargs)

        for k,v in self.TEST_ELEMENTS:
            v2 = float(v)
            c[k] = v2
            c.cas(k,v2,v)
            self.assertIs(c[k], v)

    def testPop(self, **kwargs):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = self.Cache(20, eviction_callback = eviction_callback, **kwargs)

        for k,v in self.TEST_ELEMENTS:
            self.assertRaises(pycuckoocache.CacheMissError, c.pop, k)
            self.assertIs(c.pop(k, None), None)
            c[k] = v
            self.assertIs(c.pop(k), v)
            self.assertRaises(pycuckoocache.CacheMissError, c.pop, k)
            self.assertIs(c.pop(k, None), None)

    def testRehash(self):
        self.testAdd(initial_size = 1)

    def testPreallocate(self):
        self.testAdd(preallocate = True)

    def testClear(self):
        c = self.Cache(20)
        for k,v in self.TEST_ELEMENTS:
            c[k] = v
        c.clear()
        self.assertEqual(len(c), 0)
        for k,v in self.TEST_ELEMENTS:
            c[k] = v

        self.assertLessEqual(len(c), len(self.TEST_ELEMENTS))

    def testRepriorize(self):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = self.Cache(20, eviction_callback = eviction_callback)
        for k,v in self.TEST_ELEMENTS:
            c[k] = v

        c._nextprio = 0x7FFFFFFFFFFFFFFE
        c[1000] = 1000
        self.assertEqual(c.get(1000), 1000)
        self.assertEqual(c._nextprio, 0x4000000000000000)

@skipIfNotCythonized
class CuckooCacheTest(PyCuckooCacheTest):
    Cache = cuckoocache.LazyCuckooCache
    testRepriorize = None

