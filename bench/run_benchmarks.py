# -*- coding: utf-8 -*-
from __future__ import print_function

import time
import argparse
import sys
import os.path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

def sfilter(suite, key):
    return suite is None or key.startswith(suite)

def bench_all(suite, just_list, **kw):
    for bench in benchmarks.BENCHMARKS:
        if sfilter(suite, bench.bench_prefix):
            if just_list:
                print(bench.bench_prefix, "-", bench.description)
            else:
                bench.run(**kw)

def bench(args):
    if args.cputime:
        clock = time.clock
    else:
        clock = time.time
    bench_all(
        timer=clock, verbose=args.verbose, series=args.series, runtime=args.runtime,
        suite=args.bench_suite, just_list=args.list)

if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('--cpu-time', dest="cputime", action="store_true", default=False,
        help = "Use CPU time as clock, rather than real-time")
    args_parser.add_argument('-v', '--verbose', action="store_true", default=False,
        help = "Show verbose progress messages")
    args_parser.add_argument('-l', '--list', action="store_true",
        help = "List all available benchmark suites")
    args_parser.add_argument('-S', '--series', type=int, default=10,
        help = "Specify how many series of runs to execute to get runtime statistics")
    args_parser.add_argument('-T', '--runtime', type=float, default=5.0,
        help =
            "Target runtime of each series of runs. The benchmark will loop until it "
            "accumulates about this much calibrated runtime")
    args_parser.add_argument('-s', '--bench-suite', dest='bench_suite',
        help =
            "Pick a specific benchmark suite by its id prefix. A partial prefix can "
            "be supplied that may match multiple benchmarks")

    args = args_parser.parse_args()

    import benchmarks

    bench(args)

