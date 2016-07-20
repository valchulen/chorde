cdef class Future:
    cdef list _cb
    cdef object _value, _logger, _done_event, __weakref__
    cdef int _running, _cancel_pending, _cancelled

    cdef int c_done(self) except -1
    cdef c_result(self, timeout, int norecurse)

    cpdef _set_nothreads(self, value)
    cpdef set(self, value)
    cpdef _on_stuff(self, callback)
