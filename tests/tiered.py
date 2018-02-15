# -*- coding: utf-8 -*-
import unittest

from .clientbase import (CacheClientTestMixIn,
    SyncWrapperTestMixIn, RWSyncWrapperTestMixIn,
    NamespaceWrapperTestMixIn
)

class TieredTest(CacheClientTestMixIn, unittest.TestCase):
    def setUpClient(self):
        from chorde.clients.inproc import InprocCacheClient
        from chorde.clients.tiered import TieredInclusiveClient
        self.l1 = InprocCacheClient(100)
        self.l2 = InprocCacheClient(100)
        return TieredInclusiveClient(self.l1, self.l2)

    def testReadThrough(self):
        self.l2.put(15, 3, 10)
        self.assertEqual(3, self.client.get(15))

    def testWriteThrough(self):
        self.client.put(14, 4, 10)
        self.assertEqual(4, self.l1.get(14))
        self.assertEqual(4, self.l2.get(14))

    def testTTLSkip(self):
        self.l2.put(5, 1, 10)
        self.l1.put(5, 2, 2)
        self.assertEqual(1, self.client.get(5, ttl_skip = 5))

    def testPromote(self):
        self.l2.put(13, 5, 10)
        self.assertEqual(5, self.client.get(13))
        self.assertEqual(5, self.l1.get(13))

    def testPromoteCallback(self):
        notified = []
        def callback(key, rv, ttl):
            notified.append((key, rv))
        self.l2.put(12, 6, 10)
        self.assertEqual(6, self.client.get(12, promote_callback = callback))
        self.assertEqual(notified, [(12, 6)])

    def testReadThroughMulti(self):
        self.l2.put(15, 3, 10)
        self.l2.put(16, 4, 10)
        self.assertItemsEqual(
            [(14, None), (15, 3), (16, 4)],
            list(self.client.getMulti([14,15,16], None)))

    def testTTLSkipMulti(self):
        self.l2.put(5, 1, 10)
        self.l1.put(5, 2, 2)
        self.l2.put(6, 3, 10)
        self.l1.put(6, 4, 2)
        self.l2.put(7, 5, 3)
        self.l1.put(7, 6, 2)
        self.l2.put(8, 7, 7)
        self.l1.put(8, 8, 10)
        self.assertItemsEqual(
            [(4, None), (5, 1), (6, 3), (7, None), (8, 8) ],
            list(self.client.getMulti([4,5,6,7,8], None, ttl_skip = 5)))

    def testPromoteMulti(self):
        self.l2.put(13, 5, 10)
        self.assertEqual(5, self.client.get(13))
        self.assertEqual(5, self.l1.get(13))

    def testPromoteCallbackMulti(self):
        notified = []
        def callback(key, rv, ttl):
            notified.append((key, rv))
        self.l2.put(12, 6, 10)
        self.assertEqual(6, self.client.get(12, promote_callback = callback))
        self.assertEqual(notified, [(12, 6)])

    def testLRU(self):
        client = self.client
        cap = client.capacity[0]

        for i in xrange(cap):
            client.put(i,i,86400)
            self.assertEqual(client.usage[0], i+1)

        self.assertTrue(client.contains(0))
        client.put(cap,cap,86400)

        # 1 should still be on the l2
        # The touch on 0 only happens against the l1,
        # so l1 evits 1, l2 evicts 0
        self.assertTrue(client.contains(1))
        self.assertTrue(client.contains(0))

        self.assertFalse(self.l1.contains(1))
        self.assertTrue(self.l1.contains(0))

        self.assertTrue(self.l2.contains(1))
        self.assertFalse(self.l2.contains(0))

    def testLimit(self):
        client = self.client
        cap = client.capacity[0]

        self.assertEqual(client.usage[0], 0)

        for i in xrange(cap):
            client.put(i,i,86400)
            self.assertEqual(client.usage[0], i+1)

        for i in xrange(cap):
            i += cap
            client.put(i,i,86400)
            self.assertEqual(client.usage[0], cap)

    def testPurge(self):
        client = self.client
        client.put(4, 15, 0)
        client.put(5, 16, 2)
        for c in client.clients:
            self.assertIsNotNone(c.getTtl(4, None)[0])
            self.assertEqual(c.get(5), 16)
        client.purge(86400)
        for c in client.clients:
            self.assertIsNotNone(c.getTtl(4, None)[0])
            self.assertEqual(c.get(5), 16)
        client.purge(0)
        for c in client.clients:
            self.assertIsNone(c.getTtl(4, None)[0])
            self.assertEqual(c.get(5), 16)

class SyncTieredTest(SyncWrapperTestMixIn, TieredTest):
    testPurge = None

class RWSyncTieredTest(RWSyncWrapperTestMixIn, TieredTest):
    testPurge = None
