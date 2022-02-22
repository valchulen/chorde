# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import threadpool, inproc, spickle

BENCHMARKS = (
    threadpool.BENCHMARKS
    + inproc.BENCHMARKS
    + spickle.BENCHMARKS
)

__all__ = ['BENCHMARKS']
