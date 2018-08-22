# -*- coding: utf-8 -*-
"""Efficient (O(1) amortized update) Quasi-Least-Recently-Used cache"""
from random import random
import functools

from chorde.clients.base import CacheMissError

IsThreadsafe = False

class _node(object):
    __slots__ = ( 'key', 'value', 'h1', 'h2', 'prio' )

    def __init__(self, key, value, h1, h2, prio):
        self.key = key
        self.value = value
        self.h1 = h1
        self.h2 = h2
        self.prio = prio

def _hash2(seed, x):
    return hash((seed, x))

class LazyCuckooCache(object):
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
    def __init__(self, size, touch_on_read = True, eviction_callback = None,
            preallocate = True, hash1 = None, hash2 = None, initial_size = 256):
        if size <= 0:
            raise ValueError("Cannot build a size-0 cache")

        self.size = size
        self.touch_on_read = touch_on_read
        if preallocate:
            initial_size = size
        else:
            initial_size = min(initial_size, size)
        initial_size = max(1, initial_size)
        self.initial_size = initial_size
        self.table = [None] * initial_size
        self.nitems = 0
        self.eviction_callback = eviction_callback
        if hash1 is None:
            hash1 = hash
        self.hash1 = hash1
        if hash2 is None:
            self.hash2 = functools.partial(_hash2, random() * 0.98 + 0.01)
        else:
            self.hash2 = hash2
        self._nextprio = 0

    def _assign_prio(self):
        prio = self._nextprio
        self._nextprio += 1
        if prio >= 0x7FFFFFFFFFFFFFFE:
            # The garbage overfloweth, repriorize
            # by truncating the lower half of the priority range
            # shouldn't happen not even once in a blue moon
            for node in self.table:
                if node is not None:
                    nprio = node.prio
                    if nprio > 0x4000000000000000:
                        node.prio = nprio - 0x4000000000000000
                    else:
                        node.prio = 0
            self._nextprio -= 0x4000000000000000
            prio -= 0x4000000000000000
        return prio

    def __len__(self):
        return self.nitems

    def __contains__(self, key):
        table = self.table
        tsize = len(table)
        h1 = self.hash1(key)
        node = table[h1 % tsize]
        if node is not None and node.key == key:
            return True

        h2 = self.hash2(key)
        node = table[h2 % tsize]
        return node is not None and node.key == key

    def rehash(self):
        otable = self.table
        size = self.size
        ntablesize = len(otable)
        if ntablesize >= size:
            return False

        ntablesize += max(1, ntablesize / 2)
        if ntablesize > size:
            ntablesize = size

        ntable = [None] * ntablesize
        _add_node = self._add_node
        eviction_callback = self.eviction_callback

        # Some evictions might take place during rehashing
        nitems = 0
        for node in otable:
            if node is not None:
                if not _add_node(ntable, ntablesize, node, node.h1, node.h2, node.key, node.value, node.prio, 0):
                    if eviction_callback is not None:
                        eviction_callback(node.key, node.value)
                else:
                    nitems += 1
        self.table = ntable
        self.nitems = nitems

        return True

    def _add_node(self, table, tsize, node, h1, h2, key, value, prio, item_diff):
        ix1 = h1 % tsize
        tnode = table[ix1]
        if tnode is not None:
            if tnode.key == key:
                # Replacing key, no eviction callback
                if node is None:
                    tnode.value = value
                    tnode.prio = prio
                else:
                    table[ix1] = node
                return True
        else:
            # Free slot
            if node is None:
                node = _node(key, value, h1, h2, prio)
            if item_diff:
                self.nitems += item_diff
            table[ix1] = node
            return True

        ix2 = h2 % tsize
        tnode = table[ix2]
        if tnode is not None:
            if tnode.key == key:
                # Replacing key, no eviction callback
                if node is None:
                    tnode.value = value
                    tnode.prio = prio
                else:
                    table[ix2] = node
                return True
        else:
            # Free slot
            if node is None:
                node = _node(key, value, h1, h2, prio)
            if item_diff:
                self.nitems += item_diff
            table[ix2] = node
            return True

        # No room
        return False

    def iterkeys(self):
        for node in self.table:
            if node is not None:
                yield node.key

    def itervalues(self):
        for node in self.table:
            if node is not None:
                yield node.value

    def iteritems(self):
        for node in self.table:
            if node is not None:
                yield (node.key, node.value)

    def keys(self):
        return list(self.iterkeys())

    def values(self):
        return list(self.itervalues())

    def items(self):
        return list(self.iteritems())

    def __iter__(self):
        return self.iterkeys()

    def __setitem__(self, key, value):
        h1 = self.hash1(key)
        h2 = self.hash2(key)
        table = self.table
        tsize = len(table)
        prio = self._assign_prio()
        _add_node = self._add_node
        if not _add_node(table, tsize, None, h1, h2, key, value, prio, 1):
            if self.rehash():
                # Try agin
                table = self.table
                tsize = len(table)
                if _add_node(table, tsize, None, h1, h2, key, value, prio, 1):
                    return

            # No room, evict some entry, pick one of the two options randomly
            tnode1 = table[h1 % tsize]
            tnode2 = table[h2 % tsize]
            prio1 = tnode1.prio
            prio2 = tnode2.prio
            if prio1 < prio1 or (prio1 == prio2 and random() < 0.5):
                tnode = tnode1
            else:
                tnode = tnode2
            eviction_callback = self.eviction_callback
            k = tnode.key
            v = tnode.value
            tnode.key = key
            tnode.value = value
            tnode.h1 = h1
            tnode.h2 = h2
            tnode.prio = prio
            if eviction_callback is not None:
                eviction_callback(k, v)

    def __getitem__(self, key):
        table = self.table
        tsize = len(table)
        h1 = self.hash1(key)
        node = table[h1 % tsize]
        if node is not None and node.key == key:
            if self.touch_on_read:
                node.prio = self._assign_prio()
            return node.value

        h2 = self.hash2(key)
        node = table[h2 % tsize]
        if node is not None and node.key == key:
            if self.touch_on_read:
                node.prio = self._assign_prio()
            return node.value

        raise CacheMissError(key)

    def __delitem__(self, key):
        return self.pop(key)

    def cas(self, key, oldvalue, newvalue):
        table = self.table
        tsize = len(table)
        h1 = self.hash1(key)
        node = table[h1 % tsize]
        if node is not None and node.key == key:
            if node.value is oldvalue:
                node.value = newvalue
                node.prio = self._assign_prio()
                return True
            else:
                return False

        h2 = self.hash2(key)
        node = table[h2 % tsize]
        if node is not None and node.key == key:
            if node.value is oldvalue:
                node.value = newvalue
                node.prio = self._assign_prio()
                return True
            else:
                return False

        return False

    def get(self, key, deflt = None):
        table = self.table
        tsize = len(table)
        h1 = self.hash1(key)
        node = table[h1 % tsize]
        if node is not None and node.key == key:
            if self.touch_on_read:
                node.prio = self._assign_prio()
            return node.value

        h2 = self.hash2(key)
        node = table[h2 % tsize]
        if node is not None and node.key == key:
            if self.touch_on_read:
                node.prio = self._assign_prio()
            return node.value

        return deflt

    def pop(self, key, deflt = CacheMissError):
        table = self.table
        tsize = len(table)
        h1 = self.hash1(key)
        ix1 = h1 % tsize
        node = table[ix1]
        if node is not None and node.key == key:
            table[ix1] = None
            self.nitems -= 1
            return node.value

        h2 = self.hash2(key)
        ix2 = h2 % tsize
        node = table[ix2]
        if node is not None and node.key == key:
            table[ix2] = None
            self.nitems -= 1
            return node.value

        if deflt is CacheMissError:
            raise CacheMissError(key)
        else:
            return deflt

    def setdefault(self, key, deflt = None):
        for j in xrange(3):
            table = self.table
            tsize = len(table)
            h1 = self.hash1(key)
            ix1 = h1 % tsize
            node1 = table[ix1]
            if node1 is not None and node1.key == key:
                if self.touch_on_read:
                    node1.prio = self._assign_prio()
                return node1.value

            h2 = self.hash2(key)
            ix2 = h2 % tsize
            node2 = table[ix2]
            if node2 is not None and node2.key == key:
                if self.touch_on_read:
                    node2.prio = self._assign_prio()
                return node2.value

            prio = self._assign_prio()
            if node1 is None:
                table[ix1] = _node(key, deflt, h1, h2, prio)
                self.nitems += 1
            elif node2 is None:
                table[ix2] = _node(key, deflt, h1, h2, prio)
                self.nitems += 1
            elif j < 2 and self.rehash():
                # Enlarged the table, retry
                continue
            else:
                # Pick a node to evict
                if random() < 0.5:
                    tnode = node1
                else:
                    tnode = node2
                eviction_callback = self.eviction_callback
                if eviction_callback is not None:
                    eviction_callback(tnode.key, tnode.value)
                tnode.key = key
                tnode.value = deflt
                tnode.h1 = h1
                tnode.h2 = h2
                tnode.prio = prio
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
        self.table = [None] * self.initial_size
        self.nitems = 0

    def defrag(self):
        self.table = list(self.table)

    def __repr__(self):
        return "<LazyCuckooCache (%d elements, %d max)>" % (len(self), self.size)

