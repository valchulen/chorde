# -*- coding: utf-8 -*-
from __future__ import absolute_import

from past.builtins import xrange as range

from .base import Benchmark
from chorde.clients.asyncache import Future


class BenchFutureBase(Benchmark):

    calibration_runs = 10000000
    initial = 10000
    maxruns = 10000000

    bench_prefix = 'async.future.'
    description = 'async.Future.'

    def __init__(self):
        super(BenchFutureBase, self).__init__(self.description)


class BenchFutureGet(BenchFutureBase):

    bench_prefix = 'async.future.result'
    description = 'async.Future.result'

    def setup(self):
        f = Future()
        f.set(3)
        return dict(f=f)

    def calibration(self, f):
        f.result

    def func(self, f):
        f.result()


class BenchFutureWTimeout(BenchFutureBase):

    bench_prefix = 'async.future.result.wtimeout'
    description = 'async.Future.result with timeout'

    def setup(self):
        f = Future()
        f.set(3)
        return dict(f=f)

    def calibration(self, f):
        f.result

    def func(self, f):
        f.result(0)


class BenchFutureTimeout(BenchFutureBase):

    bench_prefix = 'async.future.result.timeout'
    description = 'async.Future.result timeout'

    def setup(self):
        f = Future()
        return dict(f=f)

    def calibration(self, f):
        try:
            f.result
        except:
            pass

    def func(self, f):
        try:
            f.result(0)
        except:
            pass


class BenchFutureCancelled(BenchFutureBase):

    bench_prefix = 'async.future.result.cancelled'
    description = 'async.Future.result cancelled'

    def setup(self):
        f = Future()
        f.cancel()
        f.set_running_or_notify_cancelled()
        return dict(f=f)

    def calibration(self, f):
        try:
            f.result
        except:
            pass

    def func(self, f):
        try:
            f.result()
        except:
            pass


class BenchFutureSet(BenchFutureBase):

    bench_prefix = 'async.future.set'
    description = 'async.Future.set'

    def setup(self):
        return {}

    def calibration(self):
        f = Future()
        f.set

    def func(self):
        f = Future()
        f.set(3)


BENCHMARKS = [
    BenchFutureGet(),
    BenchFutureWTimeout(),
    BenchFutureTimeout(),
    BenchFutureCancelled(),
    BenchFutureSet(),
]
