"""Efficient (O(log n) amortized update) Least-Recently-Used cache"""

class CacheMissError(KeyError):
    """Error raised when a cache miss occurs"""
    pass

cdef class _node:
    cdef unsigned int prio
    cdef unsigned int index
    cdef object key
    cdef object value

    def __init__(_node self not None, unsigned int prio, unsigned int index, object key, object value):
        self.prio = prio
        self.index = index
        self.key = key
        self.value = value

    def __richcmp__(_node self not None, _node other, int op):
        if op == 0:
            return self.prio < other.prio
        elif op == 1:
            return self.prio <= other.prio
        elif op == 2:
            return self.prio == other.prio
        elif op == 3:
            return self.prio != other.prio
        elif op == 4:
            return self.prio > other.prio
        elif op == 5:
            return self.prio >= other.prio
        else:
            return False


cdef class LRUCache:
    """Least-Recently-Used (LRU) cache.
    
    Instances of this class behave like a Python mapping type, except that
    its size is guaranteed to never surpass the assigned limit and when
    spillover occurs, the least recently used items get removed first.
    """

    cdef unsigned int next_prio
    cdef readonly unsigned int size
    cdef readonly unsigned int touch_on_read
    cdef list pqueue
    cdef dict emap
    
    def __init__(LRUCache self, unsigned int size, unsigned int touch_on_read = 1):
        self.size = size
        self.touch_on_read = touch_on_read
        self.pqueue = []
        self.emap = {}
        self.next_prio = 0

    def __len__(LRUCache self not None):
        return len(self.pqueue)
    
    def __contains__(LRUCache self not None, object key):
        return key in self.emap

    cdef void c_rehash(LRUCache self):        
        cdef _node node
        cdef unsigned int i
        cdef unsigned int bprio

        bprio = self.pqueue[0].prio
        for i from 0 <= i < len(self.pqueue):
            node = self.pqueue[i]
            node.prio = node.prio - bprio
        self.next_prio = self.next_prio - bprio

    cdef void c_decrease(LRUCache self, _node node):
        cdef unsigned int ix, l, r, sw, sz
        cdef _node anode, ln, rn, swn

        node.prio = self.next_prio
        self.next_prio = self.next_prio + 1

        if self.next_prio == 0:
            self.c_rehash()

        sz = len(self.pqueue)
        while 1:
            ix = node.index
            l  = 2 * ix + 1
            r  = 2 * ix + 2

            if r < sz:
                ln = self.pqueue[l]
                rn = self.pqueue[r]

            if r < sz and rn.prio < ln.prio:
                sw = r
                swn= rn
            elif l < sz:
                sw = l
                swn= self.pqueue[l]
            else:
                break

            self.pqueue[sw] = node
            self.pqueue[ix] = swn
            node.index = sw
            swn.index = ix

    def iterkeys(LRUCache self not None):
        return self.emap.iterkeys()

    def itervalues(LRUCache self not None):
        return LRUCacheValuesIterator(self)

    def iteritems(LRUCache self not None):
        return LRUCacheItemsIterator(self)

    def keys(LRUCache self not None):
        return self.emap.keys()

    def values(LRUCache self not None):
        return list(self.itervalues())

    def items(LRUCache self not None):
        return list(self.iteritems())

    def __iter__(LRUCache self not None):
        return self.iterkeys()

    cdef int c__setitem__(LRUCache self, object key, object val) except -1:
        cdef _node node
        cdef object oldkey, oldval

        if key in self.emap:
            node = self.emap[key]
            node.value = val
            self.c_decrease(node)
        elif len(self.pqueue) >= self.size:
            node = self.pqueue[0]
            oldkey = node.key   # delay collection of old key/value, to avoid
            oldval = node.value # firing python code and thus releasing the GIL
            del self.emap[node.key]
            node.key = key
            node.value = val
            self.emap[key] = node
            self.c_decrease(node)
        else:
            node = _node(self.next_prio, len(self.pqueue), key, val)
            self.emap[key] = node
            self.pqueue.append(node)
            self.next_prio = self.next_prio + 1
            if self.next_prio == 0:
                self.c_rehash()
        
        return 0

    def __setitem__(LRUCache self not None, object key, object val):
        self.c__setitem__(key, val)

    cdef object c__getitem__(LRUCache self, key):
        cdef _node node

        if key not in self.emap:
            raise CacheMissError(key)
        else:
            node = self.emap[key]
            if self.touch_on_read:
                self.c_decrease(node)
            return node.value
    
    def __getitem__(LRUCache self not None, key):
        return self.c__getitem__(key)

    cdef int c__delitem__(LRUCache self, key) except -1:
        cdef _node node, node2

        if key not in self.emap:
            raise CacheMissError(key)
        else:
            node = self.emap[key]
            self.c_decrease(node)

            node2 = self.pqueue[-1]
            if node2 is not node:
                self.pqueue[node.index] = node2
                node2.index = node.index

            del self.emap[key]
            del self.pqueue[-1]
            
            return 0

    def __delitem__(LRUCache self not None, key):
        self.c__delitem__(key)

    def get(LRUCache self not None, object key, object deflt = None):
        cdef _node node

        if key not in self.emap:
            return deflt
        else:
            node = self.emap[key]
            self.c_decrease(node)
            return node.value
    
    def pop(LRUCache self not None, object key, object deflt = CacheMissError):
        cdef object rv

        if key not in self.emap:
            if deflt is CacheMissError:
                raise CacheMissError(key)
            else:
                rv = deflt
        else:
            rv = self.c__getitem__(key)
            self.c__delitem__(key)
        
        return rv

    def setdefault(LRUCache self not None, object key, object deflt = None):
        cdef _node node

        if key not in self.emap:
            self.c__setitem__(key, deflt)
            return deflt
        else:
            node = self.emap[key]
            self.c_decrease(node)
            return node.value

    def update(LRUCache self not None, object iterOrDict):
        if isinstance(iterOrDict, dict) or isinstance(iterOrDict, LRUCache):
            for k,v in iterOrDict.iteritems():
                self[k] = v
        else:
            for k,v in iterOrDict:
                self[k] = v

    def clear(LRUCache self not None):
        self.pqueue = []
        self.emap = {}
        self.next_prio = 0
    
    def defrag(LRUCache self not None):
        self.pqueue = list(self.pqueue)
        self.emap = self.emap.copy()

    def __repr__(LRUCache self not None):
        return "<LRUCache (%d elements, %d max)>" % (len(self), self.size)


cdef class LRUCacheItemsIterator:
    cdef LRUCache cache
    cdef unsigned int pos

    def __init__(LRUCacheItemsIterator self not None, LRUCache cache not None):
        self.cache = cache
        self.pos = 0
    def __next__(LRUCacheItemsIterator self not None):
        cdef _node node
        if self.pos >= len(self.cache.pqueue):
            raise StopIteration
        else:
            node = self.cache.pqueue[self.pos]
            self.pos = self.pos + 1
            return ( node.key, node.value )
    def __iter__(LRUCacheItemsIterator self not None):
        return self
    
cdef class LRUCacheValuesIterator:
    cdef LRUCache cache
    cdef unsigned int pos

    def __init__(LRUCacheValuesIterator self not None, LRUCache cache not None):
        self.cache = cache
        self.pos = 0
    def __next__(LRUCacheValuesIterator self not None):
        cdef _node node
        if self.pos >= len(self.cache.pqueue):
            raise StopIteration
        else:
            node = self.cache.pqueue[self.pos]
            self.pos = self.pos + 1
            return node.value
    def __iter__(LRUCacheValuesIterator self not None):
        return self
    
