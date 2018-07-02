# -*- coding: utf-8 -*-
import collections
import thread
from threading import Event, Thread
import multiprocessing.pool
import time
import unittest

from chorde.threadpool import ThreadPool

from .base import TestCase

class ThreadpoolTest(TestCase):
    def setUp(self):
        self.pool = ThreadPool()

    def tearDown(self):
        self.join_close(self.pool, 60)

    @staticmethod
    def join_close(pool, timeout):
        pool.close()
        pool.join(timeout)

    @staticmethod
    def join_continue(pool, timeout):
        pool.join(timeout)

    def testCleanupCallbacks(self):
        if not hasattr(self.pool, 'register_cleanup_callback'):
            self.skipTest("Not implemented")
        called = set()
        def callback():
            called.add(None)
        self.pool.register_cleanup_callback(callback)
        self.pool.apply(time.time)
        time.sleep(0.01)
        self.assertTrue(called)

    def testCleanupCallbackErrors(self):
        if not hasattr(self.pool, 'register_cleanup_callback'):
            self.skipTest("Not implemented")
        def raiser():
            raise AssertionError
        self.pool.register_cleanup_callback(raiser)
        self.pool.apply(time.time, timeout=0.1)
        time.sleep(0.1)
        self.pool.apply(time.time, timeout=0.1)
        # Just not erroring out or blocking

    def testLazyStart(self):
        if not hasattr(self.pool, 'check_started'):
            self.skipTest("Not implemented")
        self.assertFalse(self.pool.check_started())
        self.pool.apply(time.time, timeout=0.1)
        self.assertTrue(self.pool.check_started())

    def testExplicitStart(self):
        if not hasattr(self.pool, 'check_started'):
            self.skipTest("Not implemented")
        self.assertFalse(self.pool.check_started())
        self.pool.start()
        self.assertTrue(self.pool.check_started())

    def testWorkerRespawnOnFork(self):
        if not hasattr(self.pool, '_ThreadPool__pid'):
            self.skipTest("Not implemented")

        self.pool.start()
        workers = getattr(self.pool, '_ThreadPool__workers')
        nworkers = len(workers)
        self.assertTrue(self.pool.is_started())
        self.assertTrue(nworkers > 0)

        # Simulate a fork (pid change, workers die)
        setattr(self.pool, '_ThreadPool__pid', 0)
        workers[0].terminate(wait=False)
        self.assertFalse(self.pool.is_started())

        # Make sure it re-starts
        self.pool.assert_started()
        self.assertTrue(self.pool.is_started())

        workers2 = getattr(self.pool, '_ThreadPool__workers')
        nworkers2 = len(workers2)
        self.assertEqual(nworkers, nworkers2)
        self.assertFalse(workers2[0] is workers[0])

    def testAsyncLatency(self):
        # Warm up the pool
        self.pool.apply(lambda:None)

        for i in xrange(100):
            t0 = time.time()
            ev = Event()
            self.pool.apply_async(ev.set)
            ev.wait()
            t1 = time.time()
            self.assertLess(t1-t0, 0.05)

    def testAsyncBlocking(self):
        # Warm up the pool
        self.pool.apply(lambda:None)

        for i in xrange(100):
            t0 = time.time()
            ev = Event()
            self.pool.apply_async(ev.set)
            t1 = time.time()
            ev.wait()
            self.assertLess(t1-t0, 0.05)

    def testSyncLatency(self):
        # Warm up the pool
        self.pool.apply(lambda:None)

        for i in xrange(100):
            t0 = time.time()
            t1 = self.pool.apply(time.time)
            self.assertLess(t1-t0, 0.05)

    def testExceptions(self):
        def raiseme():
            raise RuntimeError
        self.assertRaises(RuntimeError, self.pool.apply, raiseme)

    def testConcurrency(self):
        N = 10000
        M = 50
        counts = collections.defaultdict(int)
        def accounting(i):
            counts[thread.get_ident()] += 1
        def killit(i):
            for j in xrange(N):
                self.pool.apply_async(accounting, (i,))
        threads = [ Thread(target=killit, args=(i,)) for i in xrange(M) ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.join_continue(self.pool, 60)
        total_counts = self.pool.apply(sum, (counts.itervalues(),))
        self.assertEqual(total_counts, N*M)

    def testClose(self):
        N = 100
        M = 100
        counts = collections.defaultdict(int)
        def accounting():
            counts[thread.get_ident()] += 1

        for i in xrange(N):
            counts.clear()
            pool = ThreadPool(2)
            for j in xrange(M):
                pool.apply_async(accounting)
            pool.close()
            pool.join()
            total_counts = sum(counts.itervalues())
            self.assertEqual(total_counts, M)

    def testTerminate(self):
        N = 100
        M = 100
        def accounting(counts):
            counts[thread.get_ident()] += 1

        for i in xrange(N):
            counts = collections.defaultdict(int)
            args = (counts,)
            pool = ThreadPool(2)
            for j in xrange(M):
                pool.apply_async(accounting, args)
            pool.terminate()
            pool.join()
            total_counts = sum(counts.itervalues())
            self.assertLessEqual(total_counts, M)

class ThreadpoolSubqueueWrapperTest(ThreadpoolTest):
    def setUp(self):
        self.pool = ThreadPool().subqueue("something")

    def tearDown(self):
        self.join_close(self.pool.pool, 60)

class ThreadpoolMultiprocessingCompatiblitityTest(ThreadpoolTest):
    def setUp(self):
        self.pool = multiprocessing.pool.ThreadPool()

    @staticmethod
    def join_close(pool, timeout):
        pool.close()
        pool.join()

    @staticmethod
    def join_continue(pool, timeout):
        pool.close()
        pool.join()
        pool.__init__() # hackish, but works

    # multiprocessing.pool.ThreadPool isn't that great with latency
    testAsyncLatency = unittest.expectedFailure(ThreadpoolTest.testAsyncLatency)

class MultiQueueTest(TestCase):
    def setUp(self):
        self.pool = ThreadPool()

    def tearDown(self):
        self.pool.terminate()

    def testQueuePrio(self):
        self.pool.set_queueprio(1, "a")
        self.pool.set_queueprio(5, "b")
        self.assertEqual(1, self.pool.queueprio("a"))
        self.assertEqual(5, self.pool.queueprio("b"))

    def testFairness(self):
        # Calibrate for low-latency (needed by the test)
        # Should be fine due to zero-copy slicing
        self.pool.max_batch = 50

        terminate = []
        M = 50
        counts = collections.defaultdict(int)
        def accounting(i):
            counts[thread.get_ident()] += 1
        def killit(i):
            while not terminate:
                self.pool.apply_async(accounting, (i,))
                time.sleep(0) # needed to avoid GIL issues that skew test results
        threads = [ Thread(target=killit, args=(i,)) for i in xrange(M) ]
        for t in threads:
            t.start()
        time.sleep(0.1) # let it fill up
        terminate.append(None)
        for t in threads:
            t.join()
        t0 = time.time()
        self.pool.apply(sum, (counts.itervalues(),), queue = "Johnny")
        t1 = time.time()
        self.assertLess(t1-t0, 0.025)

    def testWeighting(self):
        # Calibrate for maximum fairness accuracy (needed by test)
        self.pool.max_batch = 100
        self.pool.min_batch = 1

        counts = collections.defaultdict(int)
        def accounting(i):
            counts[i] += 1
        self.pool.set_queueprio(3,"mean")
        self.pool.set_queueprio(1,"simple")
        qsequence = ["mean", "simple"]
        for i in xrange(10000):
            for q in qsequence:
                self.pool.apply_async(accounting, (q,), queue=q)
        countsnap = self.pool.apply(counts.copy, queue = "mean")
        self.pool.join(60)
        self.assertLess(countsnap["simple"]*2, countsnap["mean"])

    def testWrapper(self):
        # Calibrate for maximum fairness accuracy (needed by test)
        self.pool.max_batch = 100
        self.pool.min_batch = 1

        counts = collections.defaultdict(int)
        simple = self.pool.subqueue("simple", 1)
        mean = self.pool.subqueue("mean", 3)
        def accounting(i):
            counts[i] += 1
        qsequence = [(mean,"mean"), (simple,"simple")]
        for i in xrange(10000):
            for q,qname in qsequence:
                q.apply_async(accounting, (qname,))
        countsnap = mean.apply(counts.copy)
        self.pool.join(60)
        self.assertLess(countsnap["simple"]*2, countsnap["mean"])

