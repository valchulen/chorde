from __future__ import absolute_import

import os
import unittest
import tempfile
import threading

from chorde import shmemutils


class SharedCounter32Test(unittest.TestCase):

    CounterClass = shmemutils.SharedCounter32
    increment = 1

    def _make_counter(self, slots=100):
        tmppath = tempfile.mktemp()
        self.addCleanup(os.unlink, tmppath)
        return self.CounterClass.from_path(slots, tmppath)

    def test_create_empty(self):
        counter = self._make_counter()
        self.assertEqual(0, counter.value)

    def test_single_threaded(self):
        counter = self._make_counter()
        inc = self.increment
        for i in range(1000):
            self.assertEqual(i * inc, counter.value)
            counter += inc

    def test_multi_threaded(self):
        counter = self._make_counter()
        niter = 10000
        nthreads = 10
        inc = self.increment
        def increase():
            c = counter
            for i in range(niter):
                c += inc
        threads = [threading.Thread(target=increase) for _ in range(nthreads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(niter * nthreads * inc, counter.value)


class SharedCounter64Test(SharedCounter32Test):

    CounterClass = shmemutils.SharedCounter64
    increment = 1 << 25