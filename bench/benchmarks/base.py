# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function
from future.builtins import range

import time
import timeit
import functools
import math

from .utils import prettytime

class Benchmark(object):
    initial = 1000
    calibration_runs = 1000000
    maxloops = 10
    maxruns = 1000000
    bench_prefix = ''

    def __init__(self, description, **kw):
        self.description = description
        # Not calling super, because super is object and doesn't take kwargs
        # Thus, for multiple inheritance to work, make sure to always inherit
        # from Benchmark LAST

    def run(self, series = 10, runtime = 5.0, warmup = 1, timer = time.time, verbose = False, **kw):
        print("---", self.bench_prefix, '-', self.description, "---")

        initial = self.initial
        calibration_runs = self.calibration_runs
        maxloops = self.maxloops
        maxruns = self.maxruns
        max_test_overhead = runtime * 10

        percall_series = []
        for s in range(series + warmup):
            state = self.setup()

            calibration_time = timeit.timeit(
                functools.partial(self.calibration, **state),
                number=calibration_runs) / float(calibration_runs)
            if verbose and not s:
                print("    ", prettytime(calibration_time), "per run overhead")

            times = []
            runs = []
            current_runs = initial
            start_time = time.time()
            for r in range(maxloops):
                times.append(
                    max(0,
                        timeit.timeit(functools.partial(self.func, **state), number=current_runs)
                        - calibration_time * current_runs
                    )
                )
                runs.append(current_runs)
                total_runtime = sum(times)
                if total_runtime >= runtime or (time.time() - start_time) > max_test_overhead:
                    break

                total_runs = sum(runs)
                if total_runtime <= 0:
                    # Probably some snafu with calibration
                    current_runs *= 2
                else:
                    current_runs = int(max(initial, (runtime - total_runtime) / (total_runtime / total_runs)))
                current_runs = max(initial, min(current_runs, maxruns))
            self.cleanup(**state)

            if s >= warmup:
                percall_series.extend([ t/r for t,r in zip(times, runs) ])

            if verbose:
                print(
                    "    ", sum(runs), "runs in", prettytime(sum(times)),
                    " (", prettytime(sum(times)/sum(runs)), " per call)",
                    end='')
                if s >= warmup:
                    print()
                else:
                    print(" - warmup run")

        avg = sum(percall_series) / len(percall_series)
        print("  avg:", prettytime(avg))
        print("  max:", prettytime(max(percall_series)))
        print("  min:", prettytime(min(percall_series)))
        print("  std:", prettytime(math.sqrt(sum([ (pc-avg)*(pc-avg) for pc in percall_series ]) / len(percall_series))))

    def setup(self):
        return {}

    def calibration(self):
        pass

    def cleanup(self):
        pass
