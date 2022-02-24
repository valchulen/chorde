# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import threadpool, inproc, spickle, decorators

BENCHMARKS = (
    threadpool.BENCHMARKS
    + inproc.BENCHMARKS
    + spickle.BENCHMARKS
    + decorators.BENCHMARKS
)

__all__ = ['BENCHMARKS']
