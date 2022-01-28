# -*- coding: utf-8 -*-
times = [
    (1, 's', '%.3f'),
    (0.001, 'ms', '%.3f'),
    (0.000001, 'us', '%.3f'),
    (0.000000001, 'ns', '%.3f'),
]

def prettygen(size, separator, scales, margin):
    for scale, unit, fmt in scales:
        if size > (margin * scale):
            break
    return separator.join([fmt % (float(size) / (scale if scale > 0 else 1)), unit])

def prettytime(size, separator = ' '):
    return prettygen(size, separator, times, 2)
