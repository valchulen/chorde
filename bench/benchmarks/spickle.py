# -*- coding: utf-8 -*-
from __future__ import absolute_import

from random import random, choice

from .base import Benchmark
from chorde import sPickle


class BenchDump(Benchmark):

    key = b"blarg"
    bench_prefix = 'spickle.dumps'

    def __init__(self):
        super(BenchDump, self).__init__(prefix='', description='sPickle.dumps')

    def setup(self):
        return dict(
            payload="a" * 1000
        )

    def calibration(self, payload):
        sPickle.dumps

    def func(self, payload):
        sPickle.dumps(self.key, payload)

class BenchLoad(Benchmark):

    key = b"blarg"
    bench_prefix = 'spickle.loads'

    def __init__(self):
        super(BenchLoad, self).__init__(description='sPickle.loads')

    def setup(self):
        return dict(
            payload=sPickle.dumps(self.key, "a" * 1000)
        )

    def calibration(self, payload):
        self.key
        sPickle.loads

    def func(self, payload):
        sPickle.loads(self.key, payload)


BENCHMARKS = [
    BenchDump(),
    BenchLoad(),
]
