# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest
import gc
import weakref

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

    def _build_with_values(self, size, **kwargs):
        c = self.Cache(size, **kwargs)
        for k,v in self.TEST_ELEMENTS:
            c[k] = v
        return c

    def testSize0(self):
        self.assertRaises(ValueError, self.Cache, 0)

    def testAdd(self, **kwargs):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = self._build_with_values(20, eviction_callback = eviction_callback, **kwargs)

        self.assertEqual(len(evictions) + len(c), len(self.TEST_ELEMENTS))
        overflow = dict(evictions)
        for k,v in self.TEST_ELEMENTS:
            self.assertEqual(c.get(k, overflow.get(k)), v)
            if k in c:
                self.assertEqual(c[k], v)

    def testNoCallback(self):
        c = self._build_with_values(20)

        self.assertLessEqual(len(c), len(self.TEST_ELEMENTS))
        for k,v in self.TEST_ELEMENTS:
            if k in c:
                self.assertEqual(c.get(k), v)
                self.assertEqual(c[k], v)
                self.assertIs(c.setdefault(k, None), v)

    def testContains(self):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = self.Cache(20, eviction_callback = eviction_callback)

        for k,v in self.TEST_ELEMENTS:
            self.assertFalse(k in c)

        for k,v in self.TEST_ELEMENTS:
            c[k] = v

        self.assertEqual(len(evictions) + len(c), len(self.TEST_ELEMENTS))
        overflow = dict(evictions)
        for k,v in self.TEST_ELEMENTS:
            self.assertTrue(k in c or k in overflow)

    def testSetDefault(self, min_size = 4, **kwargs):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = self.Cache(5, eviction_callback = eviction_callback, **kwargs)

        for k,v in self.TEST_ELEMENTS:
            # Construct a distinct object
            v = float(v)
            self.assertIs(c.setdefault(k,v), v)

            # Second time returns the first one
            v2 = float(v)
            self.assertIs(c.setdefault(k,v2), v)

        self.assertGreaterEqual(len(c), min_size)
        self.assertGreater(len(evictions), 0)

    def testCas(self):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = self.Cache(5, eviction_callback = eviction_callback)

        for k,v in self.TEST_ELEMENTS:
            v2 = float(v)
            c[k] = v2
            c.cas(k,v2,v)
            self.assertIs(c[k], v)

        self.assertGreater(len(evictions), 0)

    def testPop(self):
        c = self.Cache(20)

        for k,v in self.TEST_ELEMENTS:
            self.assertRaises(pycuckoocache.CacheMissError, c.pop, k)
            self.assertIs(c.pop(k, None), None)
            c[k] = v
            self.assertIs(c.pop(k), v)
            self.assertRaises(pycuckoocache.CacheMissError, c.pop, k)
            self.assertIs(c.pop(k, None), None)

    def testRehash(self):
        self.testAdd(initial_size = 1)

    def testRehashSetDefault(self):
        self.testSetDefault(initial_size = 1)

    def testPreallocate(self):
        self.testAdd(preallocate = True)

    def testClear(self):
        c = self._build_with_values(20)
        c.clear()
        self.assertEqual(len(c), 0)
        for k,v in self.TEST_ELEMENTS:
            c[k] = v

        self.assertGreater(len(c), 0)
        self.assertLessEqual(len(c), len(self.TEST_ELEMENTS))

    def testUpdate(self, what = TEST_ELEMENTS):
        c = self.Cache(20)
        c.update(what)
        self.assertGreater(len(c), 0)
        self.assertLessEqual(len(c), len(self.TEST_ELEMENTS))
        self.assertLessEqual(set(c.iteritems()), set(self.TEST_ELEMENTS))

    def testUpdateDict(self):
        self.testUpdate(dict(self.TEST_ELEMENTS))

    def testUpdateCuckooCache(self):
        self.testUpdate(self._build_with_values(20))

    def testReplaceUpdate(self):
        c = self.Cache(20)
        c.update([(k,None) for k,v in self.TEST_ELEMENTS])
        c.update(self.TEST_ELEMENTS)
        self.assertGreater(len(c), 0)
        self.assertLessEqual(len(c), len(self.TEST_ELEMENTS))
        self.assertLessEqual(set(c.iteritems()), set(self.TEST_ELEMENTS))

    def testSelfUpdate(self):
        c = self.Cache(20)
        c[1] = 3
        c[2] = 4
        i1 = c.items()
        c.update(c)
        i2 = c.items()
        self.assertEqual(i1, i2)

    def testSelfReference(self):
        c = self.Cache(20)
        wc = weakref.ref(c)
        c[1] = 3
        c[2] = 4
        c[3] = c
        self.assertIsNotNone(wc())
        del c
        gc.collect()
        self.assertIsNone(wc())

    def testReplace(self):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = self._build_with_values(20, eviction_callback = eviction_callback)
        for k,v in self.TEST_ELEMENTS:
            c[k] = v
            self.assertIs(c[k], v)

            # Replace item without invoking the eviction callback
            old_evictions = len(evictions)
            v2 = float(v)
            c[k] = v2
            self.assertIs(c[k], v2)
            self.assertEqual(old_evictions, len(evictions))

    def testCustomHashes(self):
        h1 = lambda x : hash(str(x))
        h2 = lambda x : hash("blah" + str(x))
        c = self.testAdd(hash1 = h1, hash2 = h2)

    def testNoTouch(self):
        c = self.testAdd(touch_on_read = False)

    def testIters(self):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = self._build_with_values(20, eviction_callback = eviction_callback)
        self.assertEqual(len(c), len(c.keys()))
        self.assertEqual(len(c), len(c.values()))
        self.assertEqual(len(c), len(c.items()))
        self.assertEqual(zip(c.keys(), c.values()), c.items())
        self.assertEqual(zip(c.iterkeys(), c.itervalues()), list(c.iteritems()))
        self.assertEqual(c.keys(), list(c.iterkeys()))
        self.assertEqual(c.values(), list(c.itervalues()))
        self.assertEqual(c.items(), list(c.iteritems()))
        self.assertEqual({k for k,v in self.TEST_ELEMENTS}, set(c.keys()) | {k for k,v in evictions})
        self.assertEqual({k for k,v in self.TEST_ELEMENTS}, set(c.iterkeys()) | {k for k,v in evictions})
        self.assertEqual({v for k,v in self.TEST_ELEMENTS}, set(c.values()) | {v for k,v in evictions})
        self.assertEqual({v for k,v in self.TEST_ELEMENTS}, set(c.itervalues()) | {v for k,v in evictions})
        self.assertEqual(set(self.TEST_ELEMENTS), set(c.items()) | set(evictions))
        self.assertEqual(set(self.TEST_ELEMENTS), set(c.iteritems()) | set(evictions))

    def testRepriorize(self):
        evictions = []
        eviction_callback = lambda k,v : evictions.append((k,v))
        c = self._build_with_values(20, eviction_callback = eviction_callback)
        c._nextprio = 0x7FFFFFFFFFFFFFFE
        c[1000] = 1000
        self.assertEqual(c.get(1000), 1000)
        self.assertEqual(c._nextprio, 0x4000000000000000)

@skipIfNotCythonized
class CuckooCacheTest(PyCuckooCacheTest):
    Cache = cuckoocache.LazyCuckooCache
    testRepriorize = None

