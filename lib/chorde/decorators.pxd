cimport cython

@cython.no_gc
cdef class CacheStats(object):
    cdef public long long hits
    cdef public long long misses
    cdef public long long errors
    cdef public long long sync_misses
    cdef public long long sync_errors
    cdef public double min_miss_time
    cdef public double max_miss_time
    cdef public double sum_miss_time
    cdef public double sum_miss_time_sq
    cdef public double wait_time
    cdef public list miss_time_histogram
    cdef public double miss_time_histogram_max
    cdef public long miss_time_histogram_bins

    @cython.locals(hbins=long)
    cpdef add_histo(self, double time)
