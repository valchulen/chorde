# -*- coding: utf-8 -*-
import time
import unittest
import threading
import functools

from .clientbase import CacheClientTestMixIn, CacheMissError, TimeoutError

class FutureTest(unittest.TestCase):
    def testTimeout(self):
        from chorde.clients.async import Future
        f = Future()
        self.assertRaises(TimeoutError, f.result, 0.1)

    def testFastTimeout(self):
        from chorde.clients.async import Future
        f = Future()
        self.assertRaises(TimeoutError, f.result, 0)

    def testPreSet(self):
        from chorde.clients.async import Future
        f = Future()
        f.set(1)
        self.assertEqual(f.result(0), 1)

    def testMiss(self):
        from chorde.clients.async import Future
        f = Future()
        f.miss()
        self.assertRaises(CacheMissError, f.result, 0)

    def testExc(self):
        from chorde.clients.async import Future
        f = Future()
        f.exc((RuntimeError,RuntimeError(1),None))
        self.assertRaises(RuntimeError, f.result, 0)

    def testAny(self):
        from chorde.clients.async import Future
        rv = []
        def exc(exc):
            rv.append("exc")
        def value(val):
            rv.append("val")
        def miss():
            rv.append("miss")
        f = Future()
        f.on_any(value,miss,exc)
        f.exc((RuntimeError,RuntimeError(1),None))
        self.assertEquals(["exc"], rv)

    def testAnyOnce(self):
        from chorde.clients.async import Future
        rv = []
        def exc(exc):
            rv.append("exc")
        def value(val):
            rv.append("val")
        def miss():
            rv.append("miss")
        def badcall():
            rv.append("bad")
        f = Future()
        f.on_any_once(value,miss,exc)
        f.on_any_once(badcall,badcall,badcall)
        f.set(3)
        self.assertEquals(["val"], rv)

class AsyncTest(CacheClientTestMixIn, unittest.TestCase):
    # Hard to guarantee LRU logic with an async writing queue
    is_lru = False

    # Capacity is a tuple, and lazy-init makes it not stable, so can't test that
    meaningful_capacity = False

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

    def testReentrantChainFutures(self):
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
        client = self.client
        client.put(1, sleepit, 120) # Delay the pool
        def putit():
            client.put(4, d2, 120)
            return 4
        d3 = Defer(putit)
        d3.future = Future()
        self.client.put(3, d3, 120)
        self.client.put(4, d1, 120)
        self.assertTrue(self.client.contains(3))
        self.assertEquals(d3.future.result(1), 4)
        self.assertTrue(self.client.contains(4))
        self.assertEquals(d1.future.result(1), 2)
        self.assertEquals(d2.future.result(1), 2)
        self.assertTrue(self.client.contains(4))
        self.assertEquals(self.client.get(4), 2)

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

class AsyncCustomPoolTest(AsyncTest):
    # Capacity is a tuple, and lazy-init makes it not stable, so can't test that
    meaningful_capacity = False

    def setUpClient(self):
        from chorde.clients.inproc import InprocCacheClient
        from chorde.clients.async import AsyncWriteCacheClient
        from chorde.threadpool import ThreadPool
        return AsyncWriteCacheClient(InprocCacheClient(100), 100, 1,
            threadpool = functools.partial(ThreadPool(1).subqueue, "meh"))


class AsyncProcessorTest(unittest.TestCase):
    def setUp(self):
        from chorde.clients.async import AsyncCacheProcessor
        from chorde.clients.inproc import InprocCacheClient
        self.client = AsyncCacheProcessor(1, InprocCacheClient(100))

    def testBasic(self):
        self.client.put(1, 2, 120).result(1)
        self.assertTrue(self.client.contains(1).result(1))
        self.assertFalse(self.client.contains(2).result(1))
        self.assertEquals(self.client.get(1).result(1), 2)
        self.assertRaises(CacheMissError, self.client.get(2).result, 1)

    def testParallel(self):
        # don't wait for it, we have 1 thread, it should respect FIFO
        self.client.put(1, 2, 120)
        contains1 = self.client.contains(1)
        contains2 = self.client.contains(2)
        get1 = self.client.get(1)
        get2 = self.client.get(2)

        self.assertTrue(contains1.result(1))
        self.assertFalse(contains2.result(1))
        self.assertEquals(get1.result(1), 2)
        self.assertRaises(CacheMissError, get2.result, 1)

    def testCoalescence(self):
        # Poor man's mock
        oldget = self.client.client.get
        numget = [0]
        def newget(*p, **kw):
            numget[0] += 1
            return oldget(*p, **kw)
        self.client.client.get = newget

        def sleepit():
            time.sleep(0.1)

        self.client.do_async(sleepit) # just slow down the pool
        self.client.get(2)
        self.client.get(2)
        self.client.get(2)
        self.assertRaises(CacheMissError, self.client.get(2).result, 1)
        self.assertEqual(numget[0], 1)

class AsyncProcessorCustomPoolTest(AsyncProcessorTest):
    def setUp(self):
        from chorde.clients.async import AsyncCacheProcessor
        from chorde.clients.inproc import InprocCacheClient
        from chorde.threadpool import ThreadPool
        self.client = AsyncCacheProcessor(1, InprocCacheClient(100),
            threadpool = functools.partial(ThreadPool(1).subqueue, "meh"))
