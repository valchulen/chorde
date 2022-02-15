# -*- coding: utf-8 -*-
from __future__ import absolute_import

import threading
from random import random, choice
from past.builtins import xrange as range

from .base import Benchmark
from chorde.clients.inproc import InprocCacheClient, CuckooCache


class BenchInprocBase(Benchmark):

    ClientClass = InprocCacheClient
    ClientName = 'lru'
    MethodName = 'base'

    def __init__(self, prefix=[], size=100, **kw):
        self.bench_prefix = '.'.join(["inproc", self.MethodName, self.ClientName] + prefix + ['sz%d' % size])
        self.size = size
        super(BenchInprocBase, self).__init__(
            description = self.bench_prefix,
            **kw)

    def setup(self):
        return dict(
            c=self.ClientClass(self.size)
        )

class BenchInprocPut(BenchInprocBase):

    MethodName = 'put'

    def calibration(self, c):
        random()
        c.put

    def func(self, c):
        c.put(random(), 5, 60)

    def cleanup(self, c):
        c.clear()


class BenchInprocGet(BenchInprocBase):

    MethodName = 'get'

    def setup(self):
        c = self.ClientClass(self.size)
        k = set()
        for i in range(self.size):
            k.add(i)
            c.put(i, i, 86400)
        return dict(c=c, keys=list(k))

    def calibration(self, c, keys):
        choice(keys)
        c.get

    def func(self, c, keys):
        c.get(choice(keys), None)

    def cleanup(self, c, keys):
        c.clear()
        del keys[:]


class BenchInprocContains(BenchInprocBase):

    MethodName = 'contains'

    def setup(self):
        c = self.ClientClass(self.size)
        k = set()
        for i in range(self.size * 2):
            k.add(i)
            if i % 2:
                c.put(i, i, 86400)
        return dict(c=c, keys=list(k))

    def calibration(self, c, keys):
        choice(keys)
        c.contains

    def func(self, c, keys):
        c.contains(choice(keys), None)

    def cleanup(self, c, keys):
        c.clear()
        del keys[:]


class BenchInprocCuckooMixIn:

    ClientName = 'cuckoo'

    @staticmethod
    def ClientClass(*p, **kw):
        return InprocCacheClient(*p, store_class=CuckooCache, **kw)


class BenchInprocPutCuckoo(BenchInprocCuckooMixIn, BenchInprocPut):
    pass


class BenchInprocGetCuckoo(BenchInprocCuckooMixIn, BenchInprocGet):
    pass


class BenchInprocContainsCuckoo(BenchInprocCuckooMixIn, BenchInprocContains):
    pass


BENCHMARKS = [
    bench_class(size=size)
    for size in [100,10000]
    for bench_class in [
        BenchInprocPut,
        BenchInprocPutCuckoo,
        BenchInprocGet,
        BenchInprocGetCuckoo,
        BenchInprocContains,
        BenchInprocContainsCuckoo,
    ]
]
