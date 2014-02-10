# -*- coding: utf-8 -*-
import time
import unittest
import threading

from .clientbase import CacheClientTestMixIn

class AsyncTest(CacheClientTestMixIn, unittest.TestCase):
    # Hard to guarantee LRU logic with an async writing queue
    is_lru = False
    
    def setUpClient(self):
        from chorde.clients.inproc import InprocCacheClient
        from chorde.clients.async import AsyncWriteCacheClient
        return AsyncWriteCacheClient(InprocCacheClient(100), 100, 1)

    def testDeferreds(self):
        from chorde.clients.async import Defer
        done = []
        ev = threading.Event()
        def sleepit():
            done.append(None)
            ev.set()
            return done
        self.client.put(1, Defer(sleepit), 120)
        self.assertTrue(self.client.contains(1))
        self.assertTrue(ev.wait(1))
        self.assertTrue(self.client.contains(1))
        self.assertEquals(self.client.get(1), [None])
        self.assertEquals(done, [None])
    
    def testFutures(self):
        from chorde.clients.async import Defer, Future
        frv = Future()
        def sleepit():
            return 1
        d = Defer(sleepit)
        d.future = frv
        self.client.put(2, d, 120)
        self.assertTrue(self.client.contains(2))
        self.assertEquals(frv.result(1), 1)
        self.assertTrue(self.client.contains(2))
        self.assertEquals(self.client.get(2), 1)
        self.assertTrue(frv.done())
    
    def testChainFutures(self):
        from chorde.clients.async import Defer, Future
        def sleepit():
            time.sleep(0.1)
            return 2
        def sleepit2():
            time.sleep(0.1)
            return 3
        d1 = Defer(sleepit)
        d2 = Defer(sleepit2)
        d1.future = Future()
        d2.future = Future()
        self.client.put(1, sleepit, 120) # Delay the pool
        self.client.put(3, d1, 120)
        self.client.put(3, d2, 120)
        self.assertTrue(self.client.contains(3))
        self.assertEquals(d1.future.result(1), 2)
        self.assertEquals(d2.future.result(1), 2)
        self.assertTrue(self.client.contains(3))
        self.assertEquals(self.client.get(3), 2)
    
    def testStepFutures(self):
        from chorde.clients.async import Defer, Future
        def sleepit():
            time.sleep(0.1)
            return 2
        d1 = Defer(sleepit)
        d1.future = Future()
        self.client.put(1, sleepit, 120) # Delay the pool
        self.client.put(4, d1, 120)
        self.client.put(4, 7, 120)
        self.assertTrue(self.client.contains(4))
        self.assertEquals(d1.future.result(1), 7)
        self.assertTrue(self.client.contains(4))
        self.assertEquals(self.client.get(4), 7)

    # Purge is asynchronous and purges the writing queue first, 
    # so guaranteeing sync semantics isn't really efficient
    testPurge = unittest.expectedFailure(CacheClientTestMixIn.testPurge)

    # Testing limits cannot be done deterministically because of the
    # concurrent worker, so don't bother
    testLimit = unittest.skip("non-deterministic")(CacheClientTestMixIn.testLimit)
