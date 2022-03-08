from __future__ import absolute_import

import unittest
import threading

from chorde import serialize


class RWLockTest(unittest.TestCase):

    LockClass = serialize.ReadWriteLock

    def _get_fixture(self):
        lockobj = self.LockClass()

        counters = {0: 0}

        @serialize.serialize_read(lockobj=lockobj)
        def get():
            return counters[0]

        @serialize.serialize_write(lockobj=lockobj)
        def inc():
            counters[0] += 1

        return get, inc

    def test_sequential(self):
        get, inc = self._get_fixture()

        for _ in range(1000):
            inc()

        self.assertEqual(get(), 1000)

    def test_concurrent_reads(self):
        get, inc = self._get_fixture()

        stop = []

        def getmuch():
            while not stop:
                get()

        threads = [threading.Thread(target=getmuch) for _ in range(4)]
        for t in threads:
            t.start()
        for _ in range(1000):
            inc()

        stop.append(True)
        for t in threads:
            t.join()

        self.assertEqual(get(), 1000)

    def test_concurrent_writes(self):
        get, inc = self._get_fixture()

        stop = []

        def writemuch():
            for _ in range(10000):
                inc()

        wthreads = [threading.Thread(target=writemuch) for _ in range(4)]
        for t in wthreads:
            t.start()
        for t in wthreads:
            t.join()

        self.assertEqual(get(), 40000)

    def test_concurrent_both(self):
        get, inc = self._get_fixture()

        stop = []

        def getmuch():
            while not stop:
                get()

        def writemuch():
            for _ in range(10000):
                inc()

        rthreads = [threading.Thread(target=getmuch) for _ in range(4)]
        wthreads = [threading.Thread(target=writemuch) for _ in range(4)]
        for t in rthreads + wthreads:
            t.start()
        for t in wthreads:
            t.join()

        stop.append(True)
        for t in rthreads:
            t.join()

        self.assertEqual(get(), 40000)


class RWRLockTest(RWLockTest):

    LockClass = serialize.ReadWriteRLock


class InstanceLockTest(RWLockTest):

    @serialize.serialize_read()
    def get(self):
        return self.counters[0]

    @serialize.serialize_write()
    def inc(self):
        self.counters[0] += 1

    def _get_fixture(self):
        self.counters = {0: 0}

        return self.get, self.inc
