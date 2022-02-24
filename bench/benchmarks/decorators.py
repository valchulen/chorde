# -*- coding: utf-8 -*-
from __future__ import absolute_import

from random import random, choice

from .base import Benchmark
from chorde import decorators
from chorde.clients import inproc


class BenchSimpleDecorator(Benchmark):

    key = b"blarg"
    bench_prefix = 'decorators.simple'
    ttl = 3600
    initial = 200

    def __init__(self, size, rndspace):
        self.rndspace = rndspace
        self.size = size
        self.bench_prefix = self.bench_prefix + '.sz%d_rnd%d' % (size, rndspace)
        super(BenchSimpleDecorator, self).__init__(
            description='Simple decorator, lru size %d, argument space %d' % (size, rndspace))

    @staticmethod
    def callmode(func):
        return func

    def setup(self):
        @decorators.cached(inproc.InprocCacheClient(self.size), ttl=self.ttl)
        def func(x):
            return x
        
        return dict(
            f=self.callmode(func)
        )

    def calibration(self, f):
        int(random() * self.rndspace)

    def func(self, f):
        f(int(random() * self.rndspace))


class BenchBgDecorator(BenchSimpleDecorator):

    bench_prefix = 'decorators.bg.sync'

    @staticmethod
    def callmode(func):
        return func.async()


class BenchBgLazyDecorator(BenchSimpleDecorator):

    bench_prefix = 'decorators.bg.lazy'

    @staticmethod
    def callmode(func):
        func = func.async()
        def fcall(*p, **kw):
            try:
                return func(*p, **kw)
            except Exception:
                pass
        return fcall


class BenchFutureDecorator(BenchSimpleDecorator):

    bench_prefix = 'decorators.future'

    @staticmethod
    def callmode(func):
        func = func.future()
        def fcall(*p, **kw):
            return func(*p, **kw).result()
        return fcall


BENCHMARKS = [
    cls(100, spc)
    for cls in (BenchSimpleDecorator, BenchBgDecorator, BenchBgLazyDecorator, BenchFutureDecorator)
    for spc in (100, 1000)
]
