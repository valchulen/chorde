
cdef class _node:
    # private, do not use
    cdef unsigned int prio
    cdef unsigned int index
    cdef object key
    cdef object value

    cdef attach(_node self, unsigned int prio, unsigned int index, object key, object value)
    cdef detach(_node self)

cdef class LRUCache:
    cdef int use_freelist
    cdef unsigned int next_prio
    cdef readonly unsigned int size
    cdef readonly unsigned int touch_on_read
    cdef list pqueue
    cdef list freelist
    cdef dict emap
    cdef object eviction_callback

    cdef void c_rehash(LRUCache self)
    cdef void c_decrease(LRUCache self, _node node) # private, do not use
    cdef int c__setitem__(LRUCache self, object key, object val) except -1
    cdef object c__getitem__(LRUCache self, key)
    cdef int c__delitem__(LRUCache self, key) except -1
    cdef c_get(LRUCache self, object key, object deflt)
