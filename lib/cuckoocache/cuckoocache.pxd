cdef struct _node

from cpython.object cimport PyObject

cdef class LazyCuckooCache:
    cdef object __weakref__

    cdef unsigned long long _nextprio

    cdef bint _rnd_data[16]
    cdef unsigned char _rnd_pos

    cdef object eviction_callback
    cdef object _hash2seed
    cdef object hash1
    cdef object hash2

    cdef _node *table

    cdef bint _rehash_in_progress

    cdef readonly unsigned int size
    cdef readonly unsigned int initial_size
    cdef unsigned int table_size
    cdef readonly unsigned int nitems

    cdef readonly bint touch_on_read

    cdef unsigned int _hash1(LazyCuckooCache self, x) except? 0xFFFFFFFF
    cdef unsigned int _hash2(LazyCuckooCache self, x) except? 0xFFFFFFFF
    cdef unsigned long long _assign_prio(LazyCuckooCache self)

    cdef int _add_node(LazyCuckooCache self, _node *table, unsigned int tsize, _node *node,
            unsigned int h1, unsigned int h2, PyObject *key, PyObject *value,
            unsigned long long prio, unsigned int item_diff, bint recheck) except -1
    cdef int _rehash(LazyCuckooCache self) except -1

    cdef int _init_rnd(self) except -1
    cdef bint _rnd(LazyCuckooCache self)

    cpdef pop(LazyCuckooCache self, key, deflt = ?)
    cpdef setdefault(self, key, deflt = ?)
    cpdef get(self, key, deflt = ?)
