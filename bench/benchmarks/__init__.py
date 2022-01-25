# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import threadpool, inproc

BENCHMARKS = (
    threadpool.BENCHMARKS
    + inproc.BENCHMARKS
)

__all__ = ['BENCHMARKS']
