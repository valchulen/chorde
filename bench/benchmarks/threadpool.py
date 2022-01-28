# -*- coding: utf-8 -*-
from __future__ import absolute_import

import threading
from past.builtins import xrange as range

from .base import Benchmark
from chorde.threadpool import ThreadPool


class BenchThreadpoolBase(Benchmark):

    def __init__(self, prefix=[], nthreads=1, **kw):
        self.bench_prefix = '.'.join(['threadpool'] + prefix + ['%dt' % nthreads])
        self.nthreads = nthreads
        super(BenchThreadpoolBase, self).__init__(
            description = self.bench_prefix,
            **kw)

    def setup(self):
        return dict(
            threadpool=ThreadPool(self.nthreads)
        )

class BenchThreadpoolLatency(BenchThreadpoolBase):

    def __init__(self, **kw):
        super(BenchThreadpoolLatency, self).__init__(prefix=['latency'], **kw)

    def calibration(self, threadpool):
        lambda:None
        threadpool.apply

    def func(self, threadpool):
        threadpool.apply(lambda:None, timeout=1)

    def cleanup(self, threadpool):
        threadpool.join(4)
        threadpool.terminate()

class BenchThreadpoolThoughput(BenchThreadpoolBase):

    calibration_runs = 100000

    def __init__(self, batchsize=100, nqueues=1, **kw):
        self.batchsize = batchsize
        self.nqueues = nqueues
        super(BenchThreadpoolThoughput, self).__init__(
            prefix=['thoughput', 'batch%d' % batchsize, 'q%d' % nqueues], **kw)

    def calibration(self, threadpool):
        t = lambda:None
        nq = self.nqueues
        for i in range(self.batchsize * nq):
            i % nq
            threadpool.apply_async
        evs = []
        for q in range(nq):
            evs.append(threading.Event())
            threadpool.apply_async
        for ev in evs:
            ev.isSet
            ev.wait

    def func(self, threadpool):
        t = lambda:None
        nq = self.nqueues
        for i in range(self.batchsize * nq):
            threadpool.apply_async(t, queue=i % nq)
        evs = []
        for q in range(nq):
            ev = threading.Event()
            evs.append(ev)
            threadpool.apply_async(ev.set, queue=q)
        for ev in evs:
            if not ev.isSet():
                ev.wait(1)

    def cleanup(self, threadpool):
        threadpool.join(4)
        threadpool.terminate()

BENCHMARKS = [
    bench_class(nthreads=nthreads)
    for nthreads in [1,4]
    for bench_class in [
        BenchThreadpoolLatency,
    ]
] + [
    bench_class(nthreads=nthreads, nqueues=nqueues, batchsize=batchsize)
    for nthreads in [1,4]
    for nqueues in [1,4]
    for batchsize in [10, 100]
    for bench_class in [
        BenchThreadpoolThoughput,
    ]
]
