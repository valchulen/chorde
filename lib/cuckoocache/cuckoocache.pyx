# -*- coding: utf-8 -*-
"""Efficient (O(1) amortized update) Quasi-Least-Recently-Used cache

This implementation provides atomic operations for all single-element operations.
This includes get, set, delete, setdefault and cas.

Additionally, keys, values and items will provide an atomic snapshot of
some consistent state of the cache.
"""
cimport cython

from libc.stdlib cimport malloc, free
from cpython.ref cimport Py_CLEAR, Py_XINCREF, Py_XDECREF, Py_INCREF
from cpython.object cimport (
    PyObject_RichCompareBool, Py_EQ, PyObject, visitproc, traverseproc, inquiry, PyTypeObject)
from cpython.list cimport PyList_New

cdef extern from "Python.h":
    # Similar to cpython.list's version, but it takes a PyObject*
    # Allows us to avoid generating useless (and risky) Py_DECREF calls
    # due to reference count handling of regular <object> variables
    void PyList_SET_ITEM(object list, Py_ssize_t i, PyObject *o)

IsThreadsafe = True

from random import random
import functools

CacheMissError = KeyError
cdef object CacheMissError_ = CacheMissError

# Hash table manipulation helpers
#
# Hash nodes, when empty, will contain NULL in both the key and value
# While the code doesn't assume key is NULL iff value is NULL, it ought to be true
# at all times. Still, it's safer not to assume it.
#
# In theory, tables could be rebuilt in-place. In practice, however, the process
# is difficult, and impossible to do atomically if concurrent activity from other
# threads is allowed. So we just rebuild them in a new array that we swap with the
# old one afterwards.
#
# Hashes are 32-bit, the implementation doesn't support tables bigger than 4G items
# in any case, and doing so, while simple, would involve making nodes quite bigger,
# which doesn't seem worthwhile.
#
# Nodes contain both hashes for their key, if non-empty. While the utility of the
# hashes themselves isn't huge, and could be avoided and recomputed (they're only
# ever needed while growing the table), they help make growing the hash table
# not only faster, but also atomic. Without the hashes, we'd have to invoke
# abritrary user code to recompute them, which would break atomicity of the
# rehash procedure, and cause a lot of trouble. Having the hashes, thus, is a
# small price to pay for an atomic, lockless yet thread-safe implementation.

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

    assert size > 0

    table = <_node*>malloc(sizeof(_node) * size)
    if table == NULL:
        raise MemoryError

    _init_table_items(table, 0, size)
    return table

cdef int _free_table(_node* table, unsigned int size) except -1:
    _free_table_items(table, 0, size)
    free(table)
    return 0

cdef int _key_equals(_node* node, object key, unsigned int h1) except -1:
    cdef PyObject *nkey
    if node != NULL and node.key != NULL:
        if node.key == <PyObject*>key:
            return 1
        elif node.h1 != h1:
            return 0
        nkey = node.key
        if PyObject_RichCompareBool(<object>nkey, key, Py_EQ):
            return 1
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

    #
    # Atomicity considerations
    #
    # Each call to hash1, hash2 or key_equals will break atomicity. Thus, it is necessary
    # to consider them as atomic barriers, and assume the hash table could have changed
    # considerably across those calls. All pointers to nodes to the old table could be
    # invalid.
    #
    # To cope with that, when possible, the calls should be the very last thing done,
    # after grabbing all the information required to perform whichever operation from
    # the relevant nodes.
    #
    # When manipulation of the table is the operation to be performed, after key_equals
    # table mutation needs to be assertained. A recheck of the table pointer against
    # the local copy, and the key's object identity should suffice in most contexts
    # to be able to proceed with the operation safely, since the table never grows,
    # and operations are always local to the node.
    #
    # If concurrent mutation happens at the atomic barrier, however, the operation needs
    # to be restarted. So, no change of the hash table must be applied until after
    # the atomic barrier, to avoid leaving an inconsistent state. Sometimes, such a
    # restart isn't necessary, like when updating node hit counters (prio). Failure
    # to increment a node's hit counter isn't relevant, so a better approach in those
    # cases is to just skip it.
    #
    # Each time there's a conflict, at least one of the conflicting operation succeeds.
    # This guarantees the application will be making progress as a whole (though a thread
    # in particular might not). This is a concept akin to software-transactional memory.

    def __cinit__(self, unsigned int size, bint touch_on_read = True, eviction_callback = None,
            bint preallocate = True, hash1 = None, hash2 = None, unsigned int initial_size = 256):
        cdef _node *table

        if size <= 0:
            raise ValueError("Cannot build a size-0 cache")

        self.table = NULL
        self.size = size
        self._rehash_in_progress = False
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

        self._init_rnd()

    def __dealloc__(self):
        if self.table != NULL and self.table_size > 0:
            table = self.table
            table_size = self.table_size
            self.table = NULL
            self.table_size = 0
            _free_table(table, table_size)

    cdef unsigned int _hash1(self, x) except? 0xFFFFFFFF:
        if self.hash1 is None:
            return <unsigned int><long>hash(x)
        else:
            return <unsigned int><long>self.hash1(x)

    cdef unsigned int _hash2(self, x) except? 0xFFFFFFFF:
        if self.hash2 is None:
            # Replicates what tuple's hash does with (hash2seed, x)
            return <unsigned int>((0x345678L ^ <long>hash(x)) * 1000003L + 97531L)
        else:
            return <unsigned int><long>self.hash2(x)

    cdef unsigned long long _assign_prio(self):
        cdef unsigned int i, tsize
        cdef unsigned long long prio, nprio
        cdef _node *table

        prio = self._nextprio
        self._nextprio += 1
        if prio >= 0x7FFFFFFFFFFFFFFEULL:
            # The garbage overfloweth, repriorize
            # by truncating the lower half of the priority range
            # shouldn't happen not even once in a blue moon
            # This whole loop should be atomic (albeit perhaps not fast)
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

    @cython.cdivision(True)
    cdef int _init_rnd(self) except -1:
        self._rnd_pos = 0
        for i from 0 <= i < (sizeof(self._rnd_data) / sizeof(self._rnd_data[0])):
            self._rnd_data[i] = random() < 0.5
        return 0

    @cython.cdivision(True)
    cdef bint _rnd(self):
        # This "maybe random" helper gives us a random-ish value without invoking arbitrary
        # python code, and thus break atomicity. Perfect randomness isn't necessary, but thread-safety is.
        rpos = self._rnd_pos
        self._rnd_pos = (self._rnd_pos + 1) % (sizeof(self._rnd_data) / sizeof(self._rnd_data[0]))
        return self._rnd_data[rpos]

    @cython.cdivision(True)
    def __contains__(LazyCuckooCache self, key):
        cdef _node *node
        cdef unsigned int tsize, h1, h2

        h1 = self._hash1(key)
        table = self.table
        tsize = self.table_size
        node = table + (h1 % tsize)
        if _key_equals(node, key, h1):
            return True

        h2 = self._hash2(key)
        table = self.table
        tsize = self.table_size
        node = table + (h2 % tsize)
        return _key_equals(node, key, h1)

    cdef int _rehash(LazyCuckooCache self) except -1:
        cdef unsigned int i, tsize, ntablesize, size, nitems

        otable = self.table
        size = self.size
        tsize = ntablesize = self.table_size
        if ntablesize >= size or self._rehash_in_progress:
            return 0

        eviction_callback = self.eviction_callback
        if eviction_callback is not None:
            # Atomic barrier, so recheck table afterwards
            evict_keys = []
            evict_values = []
            otable = self.table
            tsize = ntablesize = self.table_size
            if ntablesize >= size or self._rehash_in_progress:
                return 0

        self._rehash_in_progress = True
        try:
            ntablesize += max(1, ntablesize / 2)
            if ntablesize > size:
                ntablesize = size

            ntable = _alloc_table(ntablesize)
            try:
                _init_table_items(ntable, 0, ntablesize)

                # Some evictions might take place during rehashing
                nitems = 0
                for i from 0 <= i < tsize:
                    node = otable + i
                    if node.key != NULL and node.value != NULL:
                        if not self._add_node(ntable, ntablesize, node, node.h1, node.h2,
                                node.key, node.value, node.prio, 0, False):
                            # Queue eviction notification, not an atomic barrier since we just queue it
                            # without creating new objects, so no GC and no arbitrary python code can be triggered
                            if eviction_callback is not None:
                                k = <object>node.key
                                v = <object>node.value
                                evict_keys.append(k)
                                evict_values.append(v)
                                del k, v
                        else:
                            nitems += 1
            except:
                _free_table(ntable, ntablesize)
                raise

            self.table = ntable
            self.table_size = ntablesize
            self.nitems = nitems
            _free_table(otable, tsize)
        finally:
            self._rehash_in_progress = False

        if eviction_callback is not None:
            num_evictions = len(evict_keys)
            for i from 0 <= i < num_evictions:
                eviction_callback(evict_keys[i], evict_values[i])

        return 1

    @cython.cdivision(True)
    cdef int _add_node(self, _node *table, unsigned int tsize, _node *node, unsigned int h1, unsigned int h2,
            PyObject *key, PyObject *value, unsigned long long prio, unsigned int item_diff, bint recheck) except -1:
        cdef PyObject *tkey

        if recheck:
            # Recheck-enabled add_node should always receive the instance's table at entry
            assert self.table == table
            assert self.table_size == tsize

        while True:
            if recheck:
                table = self.table
                tsize = self.table_size

            # Adds a node `node` to the table `table`. The node's attributes h1, h2, key and value must
            # always be specified, but the node itself may be omitted.
            #
            # If the table is the instance's table, and can thus receive concurrent modifications
            # at atomic barriers, recheck must be given as True, and the operation will be retried on conflicts.
            #
            # If the table is a local copy, and the operation is a rebuild, recheck must be given as False to
            # avoid equality checks, since during rebuild, equal yet different keys cannot be enocuntered. An
            # identity check will be used instead, since within a hash table, equal keys are identical keys.
            tnode = table + (h1 % tsize)
            if tnode.key != NULL:
                tkey = tnode.key
                
                if (recheck and _key_equals(tnode, <object>key, h1)) or (not recheck and tkey == key):
                    if recheck and (table != self.table or tkey != tnode.key):
                        # Conflict during equals, restart
                        continue
    
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

            tnode = table + (h2 % tsize)
            if tnode.key != NULL:
                tkey = tnode.key
                if (recheck and _key_equals(tnode, <object>key, h1)) or (not recheck and tkey == key):
                    if recheck and (table != self.table or tkey != tnode.key):
                        # Conflict during equals, restart
                        continue
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
        # Allocate the list and fill it with CPython API to avoid the risk of invoking
        # the GC implicitly through calls to append
        cdef Py_ssize_t i, tsize, rvpos
        rv = PyList_New(self.nitems)
        rvpos = 0
        table = self.table
        tsize = self.table_size
        for i from 0 <= i < tsize:
            if table[i].key != NULL:
                assert rvpos < self.nitems
                Py_XINCREF(table[i].key)
                PyList_SET_ITEM(rv, rvpos, table[i].key)
                rvpos += 1
        assert rvpos == self.nitems

        # Leaking NULL entries would cause segfaults in optimized builds (no asserts)
        # So the following should be unreachable, but keep it here to avoid hard crashes
        while rvpos < self.nitems:
            Py_INCREF(None)
            PyList_SET_ITEM(rv, rvpos, <PyObject*>None)
        return rv

    def values(self):
        cdef Py_ssize_t i, tsize, rvpos
        rv = PyList_New(self.nitems)
        rvpos = 0
        table = self.table
        tsize = self.table_size
        for i from 0 <= i < tsize:
            if table[i].value != NULL:
                assert rvpos < self.nitems
                Py_XINCREF(table[i].value)
                PyList_SET_ITEM(rv, rvpos, table[i].value)
                rvpos += 1
        assert rvpos == self.nitems

        # Leaking NULL entries would cause segfaults in optimized builds (no asserts)
        # So the following should be unreachable, but keep it here to avoid hard crashes
        while rvpos < self.nitems:
            Py_INCREF(None)
            PyList_SET_ITEM(rv, rvpos, <PyObject*>None)
        return rv

    def items(self):
        # Only way to atomically snapshot this without risk of invoking the GC in the
        # middle due to python object allocations, is to build key and value lists
        # in one go, but in separate lists. We cannot build tuples on the go,
        # since each new allocation could trigger the GC and swap the table under
        # our feet.
        cdef Py_ssize_t i, tsize, rvkpos, rvvpos
        rvk = PyList_New(self.nitems)
        rvv = PyList_New(self.nitems)
        rvkpos = rvvpos = 0
        table = self.table
        tsize = self.table_size
        for i from 0 <= i < tsize:
            assert (table[i].key == NULL) == (table[i].value == NULL)
            if table[i].key != NULL:
                assert rvkpos < self.nitems
                Py_XINCREF(table[i].key)
                PyList_SET_ITEM(rvk, rvkpos, table[i].key)
                rvkpos += 1
            if table[i].value != NULL:
                assert rvvpos < self.nitems
                Py_XINCREF(table[i].value)
                PyList_SET_ITEM(rvv, rvvpos, table[i].value)
                rvvpos += 1
        assert rvkpos == self.nitems
        assert rvvpos == self.nitems

        # Leaking NULL entries would cause segfaults in optimized builds (no asserts)
        # So the following should be unreachable, but keep it here to avoid hard crashes
        while rvkpos < self.nitems:
            Py_INCREF(None)
            PyList_SET_ITEM(rvk, rvkpos, <PyObject*>None)
        while rvvpos < self.nitems:
            Py_INCREF(None)
            PyList_SET_ITEM(rvv, rvvpos, <PyObject*>None)
        return zip(rvk, rvv)

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

    @cython.cdivision(True)
    def __setitem__(self, key, value):
        cdef unsigned int h1, h2, tsize

        h1 = self._hash1(key)
        h2 = self._hash2(key)
        table = self.table
        tsize = self.table_size
        prio = self._assign_prio()
        if not self._add_node(table, tsize, NULL, h1, h2, <PyObject*>key, <PyObject*>value, prio, 1, True):
            if self._rehash():
                # Try agin
                table = self.table
                tsize = self.table_size
                if self._add_node(table, tsize, NULL, h1, h2, <PyObject*>key, <PyObject*>value, prio, 1, True):
                    return

            # No room, evict some entry, pick one of the two options randomly
            # NOTE: Don't invoke python code until all manipulations of the table
            #   are done, or baaad things may happen
            table = self.table
            tsize = self.table_size
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

    @cython.cdivision(True)
    def __getitem__(self, key):
        cdef unsigned int tsize, h1, h2

        h1 = self._hash1(key)
        table = self.table
        tsize = self.table_size
        node = table + (h1 % tsize)
        if node.value != NULL:
            v = <object>node.value
            if _key_equals(node, key, h1):
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
            if _key_equals(node, key, h1):
                # Recheck table in case equals invoked re-entrant python code
                if self.touch_on_read and self.table == table:
                    node.prio = self._assign_prio()
                return v
            del v

        raise CacheMissError_(key)

    def __delitem__(self, key):
        self.pop(key)

    @cython.cdivision(True)
    def cas(self, key, oldvalue, newvalue):
        cdef unsigned int tsize, h1, h2
        cdef PyObject *tkey

        while 1:
            h1 = self._hash1(key)
            table = self.table
            tsize = self.table_size
            node = table + (h1 % tsize)
            tkey = node.key
            if _key_equals(node, key, h1):
                if self.table != table or node.key != tkey:
                    # Re-entrancy, restart operation
                    continue
                if node.value == <PyObject*>oldvalue:
                    _value_set(node, <PyObject*>newvalue, self._assign_prio())
                    return True
                else:
                    return False

            h2 = self._hash2(key)
            table = self.table
            tsize = self.table_size
            node = table + (h2 % tsize)
            tkey = node.key
            if _key_equals(node, key, h1):
                if self.table != table or node.key != tkey:
                    # Re-entrancy, restart operation
                    continue
                if node.value == <PyObject*>oldvalue:
                    _value_set(node, <PyObject*>newvalue, self._assign_prio())
                    return True
                else:
                    return False

            return False

    @cython.cdivision(True)
    cpdef get(self, key, deflt = None):
        cdef unsigned int tsize, h1, h2

        h1 = self._hash1(key)
        table = self.table
        tsize = self.table_size
        node = table + (h1 % tsize)
        if node.value != NULL:
            v = <object>node.value
            if _key_equals(node, key, h1):
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
            if _key_equals(node, key, h1):
                # Recheck table in case equals invoked re-entrant python code
                if self.touch_on_read and self.table == table:
                    node.prio = self._assign_prio()
                return v
            del v

        return deflt

    @cython.cdivision(True)
    cpdef pop(self, key, deflt = CacheMissError_):
        cdef unsigned int tsize, h1, h2
        cdef PyObject *tkey

        while 1:
            h1 = self._hash1(key)
            table = self.table
            tsize = self.table_size
            node = table + (h1 % tsize)
            tkey = node.key
            if _key_equals(node, key, h1):
                if self.table != table or tkey != node.key:
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
            tkey = node.key
            if _key_equals(node, key, h1):
                if self.table != table or tkey != node.key:
                    # Re-entrancy, retstart operation
                    continue
                if node.value != NULL:
                    rv = <object>node.value
                else:
                    rv = deflt
                _node_set(node, NULL, NULL, 0, 0, 0)
                self.nitems -= 1
                return rv

            if deflt is CacheMissError_:
                raise CacheMissError_(key)
            else:
                return deflt

    @cython.cdivision(True)
    cpdef setdefault(self, key, deflt = None):
        cdef unsigned int tsize, h1, h2, j

        for j from 0 <= j <= 2:
            h1 = self._hash1(key)
            table = self.table
            tsize = self.table_size
            node1 = table + (h1 % tsize)
            if node1.value != NULL:
                v = <object>node1.value
                if _key_equals(node1, key, h1):
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
                if _key_equals(node2, key, h1):
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
            elif j < 2 and self._rehash():
                # Enlarged the table, retry
                continue
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
        else:
            # Unreachable code, in theory
            raise AssertionError("unreachable")

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
        _free_table(otable, tsize)

    def defrag(self):
        pass

    def __repr__(self):
        return "<LazyCuckooCache (%d elements, %d max)>" % (len(self), self.size)

# Cython's automatically generated tp_traverse and tp_clear don't know about our
# C-level hash table and all the references it contains.
#
# So we generate aditional traverse/clear methods and associate them to the
# type object at import time, remembering the original ones to chain them.
#
# A bit hackish, maybe, but effective, and Cython gives us no other recourse.

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

        # just to keep it consistent, shouldn't matter, but better safe than sorry
        p.nitems = 0
    return 0

cdef void lazy_cuckoocache_enable_gc(PyTypeObject *t):
    if t.tp_traverse != <traverseproc>cuckoocache_traverse:
        cuckoocache_cy_traverse = t.tp_traverse
        t.tp_traverse = <traverseproc>cuckoocache_traverse
    if t.tp_clear != <inquiry>cuckoocache_clear:
        t.tp_clear = <inquiry>cuckoocache_clear

lazy_cuckoocache_enable_gc(<PyTypeObject*>LazyCuckooCache)

