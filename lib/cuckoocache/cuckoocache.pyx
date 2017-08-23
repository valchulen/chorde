# -*- coding: utf-8 -*-
"""Efficient (O(1) amortized update) Quasi-Least-Recently-Used cache"""
import cython

from libc.stdlib cimport malloc, free
from cpython.ref cimport Py_CLEAR, Py_XINCREF, Py_XDECREF, Py_INCREF
from cpython.object cimport (
    PyObject_RichCompareBool, Py_EQ, PyObject, visitproc, traverseproc, inquiry, PyTypeObject)

from random import random
import functools

from chorde.clients.base import CacheMissError

cdef struct _node:
    PyObject *key
    PyObject *value
    unsigned long long prio
    unsigned int h1
    unsigned int h2

cdef int _init_table_items(_node* table, unsigned int start, unsigned int end) except -1:
    cdef unsigned int i
    for i from start <= i < end:
        table[i].key = NULL
        table[i].value = NULL
        table[i].prio = 0
        table[i].h1 = 0
        table[i].h2 = 0
    return 0

cdef int _free_table_items(_node* table, unsigned int start, unsigned int end) except -1:
    cdef unsigned int i
    for i from start <= i < end:
        Py_CLEAR(table[i].key)
        Py_CLEAR(table[i].value)
    return 0

cdef _node* _alloc_table(unsigned int size) except NULL:
    cdef _node* table
    table = <_node*>malloc(sizeof(_node) * size)
    if table != NULL:
        _init_table_items(table, 0, size)
    else:
        raise MemoryError
    return table

cdef int _key_equals(_node* node, object key) except -1:
    if node != NULL and node.key != NULL and (node.key == <PyObject*>key
            or PyObject_RichCompareBool(<object>node.key, key, Py_EQ)):
        return 1
    else:
        return 0

cdef int _value_set(_node* node, PyObject *value, unsigned long long prio) except -1:
    cdef PyObject *t
    t = node.value

    # Must keep reference decrefs at the end to avoid invoking python code
    # with inconsistent node states
    Py_XINCREF(value)
    node.value = value
    node.prio = prio
    Py_XDECREF(t)
    return 0

cdef int _node_set(_node* node, PyObject *key, PyObject *value,
        unsigned int h1, unsigned int h2, unsigned long long prio) except -1:
    cdef PyObject *tk
    cdef PyObject *tv
    tk = node.key
    tv = node.value

    # Must keep reference decrefs at the end to avoid invoking python code
    # with inconsistent node states
    Py_XINCREF(key)
    Py_XINCREF(value)
    node.key = key
    node.value = value
    node.prio = prio
    node.h1 = h1
    node.h2 = h2
    Py_XDECREF(tk)
    Py_XDECREF(tv)
    return 0

cdef class LazyCuckooCache:
    """Quasi-Least-Recently-Used (LRU) cache.

    Instances of this class behave like a Python mapping type, except that
    its size is guaranteed to never surpass the assigned limit and when
    spillover occurs, an item likely not recently used gets removed first.

    The algorithm is O(1) worst case for lookup, and amortized O(1) for
    update. Update can only incur more than O(1) if rehashing needs to take
    place, something that can be avoided by passing preallocate = True

    The default hash functions may not be at all optimal, so you're encouraged
    to provide your own if you know the type of keys you'll be hashing.
    """
    def __cinit__(self, unsigned int size, bint touch_on_read = True, eviction_callback = None,
            bint preallocate = False, hash1 = None, hash2 = None, unsigned int initial_size = 256):
        cdef _node *table

        self.table = NULL
        self.size = size
        self.touch_on_read = touch_on_read
        if preallocate:
            initial_size = size
        else:
            initial_size = min(initial_size, size)
        initial_size = max(1, initial_size)
        self.initial_size = initial_size
        self.nitems = 0
        self.eviction_callback = eviction_callback
        self.hash1 = hash1
        self.hash2 = hash2
        if hash2 is None:
            self._hash2seed = random() * 0.98 + 0.01
        self._nextprio = 0
        self.table_size = initial_size
        self.table = _alloc_table(initial_size)

        self._rnd_pos = 0
        for i from 0 <= i < (sizeof(self._rnd_data) / sizeof(self._rnd_data[0])):
            self._rnd_data[i] = random() < 0.5

    def __dealloc__(self):
        if self.table != NULL and self.table_size > 0:
            _free_table_items(self.table, 0, self.table_size)
            free(self.table)
            self.table = NULL

    cdef unsigned int _hash1(self, x) except? 0xFFFFFFFF:
        if self.hash1 is None:
            return <unsigned int><long>hash(x)
        else:
            return <unsigned int><long>self.hash1(x)

    cdef unsigned int _hash2(self, x) except? 0xFFFFFFFF:
        cdef long mult, h, y
        if self.hash2 is None:
            # Replicates what tuple's hash does with (hash2seed, x)
            return <unsigned int>(0x345678L ^ <long>hash(x)) * 1000003L + 97531L
        else:
            return <unsigned int><long>self.hash2(x)

    cdef unsigned long long _assign_prio(self) except? 0xFFFFFFFFFFFFFFFFULL:
        cdef unsigned int i, tsize
        cdef unsigned long long prio, nprio
        cdef _node *table

        prio = self._nextprio
        self._nextprio += 1
        if prio >= 0x7FFFFFFFFFFFFFFEULL:
            # The garbage overfloweth, repriorize
            # by truncating the lower half of the priority range
            # shouldn't happen not even once in a blue moon
            table = self.table
            tsize = self.table_size
            for i from 0 <= i < tsize:
                node = table+i
                if node.key != NULL:
                    nprio = node.prio
                    if nprio > 0x4000000000000000ULL:
                        node.prio = nprio - 0x4000000000000000ULL
                    else:
                        node.prio = 0
            self._nextprio -= 0x4000000000000000ULL
            prio -= 0x4000000000000000ULL
        return prio

    def __len__(self):
        return self.nitems

    cdef int _rnd(self) except -1:
        rpos = self._rnd_pos
        self._rnd_pos = (self._rnd_pos + 1) % (sizeof(self._rnd_data) / sizeof(self._rnd_data[0]))
        return self._rnd_data[rpos]

    def __contains__(LazyCuckooCache self, key):
        cdef _node *node
        cdef unsigned int tsize, h1, h2

        h1 = self._hash1(key)
        table = self.table
        tsize = self.table_size
        node = table + (h1 % tsize)
        if _key_equals(node, key):
            return True

        h2 = self._hash2(key)
        table = self.table
        tsize = self.table_size
        node = table + (h2 % tsize)
        return _key_equals(node, key)

    cdef int _rehash(LazyCuckooCache self) except -1:
        cdef unsigned int i, tsize, ntablesize, size, nitems

        otable = self.table
        size = self.size
        tsize = ntablesize = self.table_size
        if ntablesize >= size:
            return 0

        ntablesize += max(1, ntablesize / 2)
        if ntablesize > size:
            ntablesize = size

        ntable = _alloc_table(ntablesize)
        try:
            _init_table_items(ntable, 0, ntablesize)
            eviction_callback = self.eviction_callback

            # Some evictions might take place during rehashing
            nitems = 0
            for i from 0 <= i < tsize:
                node = otable+i
                if node.key != NULL and node.value != NULL:
                    if not self._add_node(ntable, ntablesize, node, node.h1, node.h2,
                            node.key, node.value, node.prio, 0):
                        # Notify eviction, atomic barrier
                        if eviction_callback is not None:
                            k = <object>node.key
                            v = <object>node.value
                            eviction_callback(k, v)
                            del k, v
                    else:
                        nitems += 1
        except:
            _free_table_items(ntable, 0, ntablesize)
            free(ntable)
            raise

        self.table = ntable
        self.table_size = ntablesize
        self.nitems = nitems
        _free_table_items(otable, 0, tsize)
        free(otable)

        return 1

    cdef int _add_node(self, _node *table, unsigned int tsize, _node *node, unsigned int h1, unsigned int h2,
            PyObject *key, PyObject *value, unsigned long long prio, unsigned int item_diff) except -1:
        cdef unsigned int ix1, ix2

        ix1 = h1 % tsize
        tnode = table+ix1
        if tnode.key != NULL:
            if _key_equals(tnode, <object>key):
                # Replacing key, no eviction callback
                if node == NULL:
                    _value_set(tnode, value, prio)
                else:
                    _node_set(tnode, key, value, h1, h2, prio)
                return 1
        else:
            # Free slot
            if item_diff:
                self.nitems += item_diff
            _node_set(tnode, key, value, h1, h2, prio)
            return 1

        ix2 = h2 % tsize
        tnode = table+ix2
        if tnode.key != NULL:
            if _key_equals(tnode, <object>key):
                # Replacing key, no eviction callback
                if node == NULL:
                    _value_set(tnode, value, prio)
                else:
                    _node_set(tnode, key, value, h1, h2, prio)
                return 1
        else:
            # Free slot
            if item_diff:
                self.nitems += item_diff
            _node_set(tnode, key, value, h1, h2, prio)
            return 1

        # No room
        return 0

    def keys(self):
        rv = []
        table = self.table
        tsize = self.table_size
        for i from 0 <= i < tsize:
            if table[i].key != NULL:
                rv.append(<object>(table[i].key))
        return rv

    def values(self):
        rv = []
        table = self.table
        tsize = self.table_size
        for i from 0 <= i < tsize:
            if table[i].value != NULL:
                rv.append(<object>(table[i].value))
        return rv

    def items(self):
        rv = []
        table = self.table
        tsize = self.table_size
        for i from 0 <= i < tsize:
            if table[i].key != NULL and table[i].value != NULL:
                rv.append((<object>(table[i].key), <object>(table[i].value)))
        return rv

    def iterkeys(self):
        table = self.table
        tsize = self.table_size
        for i from 0 <= i < tsize:
            if table[i].key != NULL:
                k = <object>(table[i].key)
                yield k
                del k
                if self.table != table or self.table_size != tsize:
                    raise RuntimeError("LazyCuckooCache changed size during iteration")

    def itervalues(self):
        table = self.table
        tsize = self.table_size
        for i from 0 <= i < tsize:
            if table[i].value != NULL:
                k = <object>(table[i].value)
                yield k
                del k
                if self.table != table or self.table_size != tsize:
                    raise RuntimeError("LazyCuckooCache changed size during iteration")

    def iteritems(self):
        table = self.table
        tsize = self.table_size
        for i from 0 <= i < tsize:
            if table[i].key != NULL and table[i].value != NULL:
                k = <object>(table[i].key)
                v = <object>(table[i].value)
                yield (k,v)
                del k,v
                if self.table != table or self.table_size != tsize:
                    raise RuntimeError("LazyCuckooCache changed size during iteration")

    def __iter__(self):
        return self.iterkeys()

    def __setitem__(self, key, value):
        cdef unsigned int h1, h2, tsize

        h1 = self._hash1(key)
        h2 = self._hash2(key)
        table = self.table
        tsize = self.table_size
        prio = self._assign_prio()
        if not self._add_node(table, tsize, NULL, h1, h2, <PyObject*>key, <PyObject*>value, prio, 1):
            if self._rehash():
                # Try agin
                table = self.table
                tsize = self.table_size
                if self._add_node(table, tsize, NULL, h1, h2, <PyObject*>key, <PyObject*>value, prio, 1):
                    return

            # No room, evict some entry, pick one of the two options randomly
            # NOTE: Don't invoke python code until all manipulations of the table
            #   are done, or baaad things may happen
            tnode1 = table + (h1 % tsize)
            tnode2 = table + (h2 % tsize)
            prio1 = tnode1.prio
            prio2 = tnode2.prio
            if prio1 < prio1 or (prio1 == prio2 and self._rnd()):
                tnode = tnode1
            else:
                tnode = tnode2
            eviction_callback = self.eviction_callback

            if tnode.key != NULL and tnode.value != NULL:
                k = <object>tnode.key
                v = <object>tnode.value
            else:
                # Should not happen, but better safe than segfault
                k = v = None
            _node_set(tnode, <PyObject*>key, <PyObject*>value, h1, h2, prio)

            # Notify eviction, atomic barrier
            if eviction_callback is not None:
                eviction_callback(k, v)

    def __getitem__(self, key):
        cdef unsigned int tsize, h1, h2

        h1 = self._hash1(key)
        table = self.table
        tsize = self.table_size
        node = table + (h1 % tsize)
        if node.value != NULL:
            v = <object>node.value
            if _key_equals(node, key):
                # Recheck table in case equals invoked re-entrant python code
                if self.touch_on_read and self.table == table:
                    node.prio = self._assign_prio()
                return v
            del v

        h2 = self._hash2(key)
        table = self.table
        tsize = self.table_size
        node = table + (h2 % tsize)
        if node.value != NULL:
            v = <object>node.value
            if _key_equals(node, key):
                # Recheck table in case equals invoked re-entrant python code
                if self.touch_on_read and self.table == table:
                    node.prio = self._assign_prio()
                return v
            del v

        raise CacheMissError(key)

    def __delitem__(self, key):
        self.pop(key)

    def cas(self, key, oldvalue, newvalue):
        cdef unsigned int tsize, h1, h2

        while 1:
            h1 = self._hash1(key)
            table = self.table
            tsize = self.table_size
            node = table + (h1 % tsize)
            if node.value == <PyObject*>oldvalue and _key_equals(node, key):
                if self.table != table:
                    # Re-entrancy, restart operation
                    continue
                _value_set(node, <PyObject*>newvalue, self._assign_prio())
                return

            h2 = self._hash2(key)
            table = self.table
            tsize = self.table_size
            node = table + (h2 % tsize)
            if node.value == <PyObject*>oldvalue and _key_equals(node, key):
                if self.table != table:
                    # Re-entrancy, restart operation
                    continue
                _value_set(node, <PyObject*>newvalue, self._assign_prio())
                return

    def get(self, key, deflt = None):
        cdef unsigned int tsize, h1, h2

        h1 = self._hash1(key)
        table = self.table
        tsize = self.table_size
        node = table + (h1 % tsize)
        if node.value != NULL:
            v = <object>node.value
            if _key_equals(node, key):
                # Recheck table in case equals invoked re-entrant python code
                if self.touch_on_read and self.table == table:
                    node.prio = self._assign_prio()
                return v
            del v

        h2 = self._hash2(key)
        table = self.table
        tsize = self.table_size
        node = table + (h2 % tsize)
        if node.value != NULL:
            v = <object>node.value
            if _key_equals(node, key):
                # Recheck table in case equals invoked re-entrant python code
                if self.touch_on_read and self.table == table:
                    node.prio = self._assign_prio()
                return v
            del v

        return deflt

    def pop(self, key, deflt = CacheMissError):
        cdef unsigned int tsize, h1, h2

        rv = None
        while 1:
            del rv
            h1 = self._hash1(key)
            table = self.table
            tsize = self.table_size
            node = table + (h1 % tsize)
            if _key_equals(node, key):
                if self.table != table:
                    # Re-entrancy, retstart operation
                    continue
                if node.value != NULL:
                    rv = <object>node.value
                else:
                    rv = deflt
                _node_set(node, NULL, NULL, 0, 0, 0)
                self.nitems -= 1
                return rv

            h2 = self._hash2(key)
            table = self.table
            tsize = self.table_size
            node = table + (h2 % tsize)
            if _key_equals(node, key):
                if self.table != table:
                    # Re-entrancy, retstart operation
                    continue
                if node.value != NULL:
                    rv = <object>node.value
                else:
                    rv = deflt
                _node_set(node, NULL, NULL, 0, 0, 0)
                self.nitems -= 1
                return rv

            if deflt is CacheMissError:
                raise CacheMissError(key)
            else:
                return deflt

    def setdefault(self, key, deflt = None):
        cdef unsigned int tsize, h1, h2

        h1 = self._hash1(key)
        table = self.table
        tsize = self.table_size
        node1 = table + (h1 % tsize)
        if node1.value != NULL:
            v = <object>node1.value
            if _key_equals(node1, key):
                # Recheck table in case equals invoked re-entrant python code
                if self.touch_on_read and self.table == table:
                    node1.prio = self._assign_prio()
                return v
            del v

        h2 = self._hash2(key)
        table = self.table
        tsize = self.table_size
        node2 = table + (h2 % tsize)
        if node2.value != NULL:
            v = <object>node2.value
            if _key_equals(node2, key):
                # Recheck table in case equals invoked re-entrant python code
                if self.touch_on_read and self.table == table:
                    node2.prio = self._assign_prio()
                return v
            del v

        prio = self._assign_prio()
        if node1.key == NULL or node1.value == NULL:
            _node_set(node1, <PyObject*>key, <PyObject*>deflt, h1, h2, prio)
            self.nitems += 1
        elif node2.key == NULL or node2.value == NULL:
            _node_set(node2, <PyObject*>key, <PyObject*>deflt, h1, h2, prio)
            self.nitems += 1
        else:
            # Pick a node to evict
            if self._rnd():
                tnode = node1
            else:
                tnode = node2
            eviction_callback = self.eviction_callback
            if tnode.key != NULL and tnode.value != NULL:
                k = <object>tnode.key
                v = <object>tnode.value
            else:
                # Should not happen, but better safe than segfault
                k = v = None
            _node_set(tnode, <PyObject*>key, <PyObject*>deflt, h1, h2, prio)
            if eviction_callback is not None:
                eviction_callback(k, v)
        return deflt

    def update(self, iterOrDict):
        if self is iterOrDict:
            return
        if isinstance(iterOrDict, dict) or isinstance(iterOrDict, LazyCuckooCache):
            for k,v in iterOrDict.iteritems():
                self[k] = v
        else:
            for k,v in iterOrDict:
                self[k] = v

    def clear(self):
        cdef unsigned int tsize

        otable = self.table
        tsize = self.table_size
        self.table = _alloc_table(self.initial_size)
        self.table_size = self.initial_size
        self.nitems = 0
        _free_table_items(otable, 0, tsize)
        free(otable)

    def defrag(self):
        pass

    def __repr__(self):
        return "<LazyCuckooCache (%d elements, %d max)>" % (len(self), self.size)

cdef traverseproc cuckoocache_cy_traverse = NULL
cdef inquiry cuckoocache_cy_clear = NULL

cdef int cuckoocache_traverse(PyObject *o, visitproc visit, void *arg):
    cdef LazyCuckooCache p = <LazyCuckooCache>o
    cdef int e
    cdef unsigned int i, tsize
    if cuckoocache_cy_traverse != NULL:
        e = cuckoocache_cy_traverse(o, visit, arg)
        if e:
            return e
    if p.table != NULL and p.table_size > 0:
        tsize = p.table_size
        table = p.table
        for i from 0 <= i < tsize:
            node = table + i
            if node.key != NULL:
                e = visit(node.key, arg)
                if e:
                    return e
            if node.value != NULL:
                e = visit(node.value, arg)
                if e:
                    return e
    return 0

cdef int cuckoocache_clear(o):
    cdef LazyCuckooCache p = <LazyCuckooCache>o
    cdef int e
    cdef unsigned int i, tsize
    if cuckoocache_cy_clear != NULL:
        e = cuckoocache_cy_clear(p)
        if e:
            return e
    if p.table != NULL and p.table_size > 0:
        _free_table_items(p.table, 0, p.table_size)
    return 0

cdef void lazy_cuckoocache_enable_gc(PyTypeObject *t):
    if t.tp_traverse != cuckoocache_traverse:
        cuckoocache_cy_traverse = t.tp_traverse
        t.tp_traverse = cuckoocache_traverse
    if t.tp_clear != cuckoocache_clear:
        t.tp_clear = cuckoocache_clear

lazy_cuckoocache_enable_gc(<PyTypeObject*>LazyCuckooCache)

