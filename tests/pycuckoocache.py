# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest

from chorde import pycuckoocache

class PyCuckooCacheTest(unittest.TestCase):
    TEST_ELEMENTS = zip(range(10), range(10,20))

    def testAdd(self, **kwargs):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = pycuckoocache.LazyCuckooCache(20, eviction_callback = eviction_callback, **kwargs)
        for k,v in self.TEST_ELEMENTS:
            c[k] = v

        self.assertEqual(len(evictions) + len(c), len(self.TEST_ELEMENTS))
        overflow = dict(evictions)
        for k,v in self.TEST_ELEMENTS:
            self.assertEqual(c.get(k, overflow.get(k)), v)

    def testRehash(self):
        self.testAdd(initial_size = 1)

    def testPreallocate(self):
        self.testAdd(preallocate = True)

    def testRepriorize(self):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = pycuckoocache.LazyCuckooCache(20, eviction_callback = eviction_callback)
        for k,v in self.TEST_ELEMENTS:
            c[k] = v

        c._nextprio = 0x7FFFFFFFFFFFFFFE
        c[1000] = 1000
        self.assertEqual(c.get(1000), 1000)
        self.assertEqual(c._nextprio, 0x4000000000000000)
