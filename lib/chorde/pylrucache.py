"""Efficient (O(log n) amortized update) Least-Recently-Used cache"""

CacheMissError = KeyError

class _node(object):
    __slots__ = ( 'prio', 'index', 'key', 'value' )

    def __init__(self, prio, index, key, value, int=int):
        self.prio = int(prio)
        self.index = int(index)
        self.key = key
        self.value = value

    def __richcmp__(self, other, op):
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


class LRUCache(object):
    """Least-Recently-Used (LRU) cache.
    
    Instances of this class behave like a Python mapping type, except that
    its size is guaranteed to never surpass the assigned limit and when
    spillover occurs, the least recently used items get removed first.
    """
    def __init__(self, size, touch_on_read = True, eviction_callback = None):
        self.size = size
        self.touch_on_read = touch_on_read
        self.pqueue = []
        self.emap = {}
        self.next_prio = 0
        self.eviction_callback = eviction_callback

    def __len__(self):
        return len(self.pqueue)
    
    def __contains__(self, key):
        return key in self.emap

    def rehash(self):
        bprio = self.pqueue[0].prio
        for i in xrange(len(self.pqueue)):
            node = self.pqueue[i]
            node.prio -= bprio
        self.next_prio -= bprio

    def decrease(self, node):
        node.prio = self.next_prio
        self.next_prio += 1

        if self.next_prio > 0xFFFFFFF0:
            self.rehash()

        pqueue = self.pqueue
        sz = len(pqueue)
        while 1:
            ix = node.index
            l  = 2 * ix + 1
            r  = 2 * ix + 2

            if r < sz:
                ln = pqueue[l]
                rn = pqueue[r]

            if r < sz and rn.prio < ln.prio:
                sw = r
                swn= rn
            elif l < sz:
                sw = l
                swn= pqueue[l]
            else:
                break

            pqueue[sw] = node
            pqueue[ix] = swn
            node.index = sw
            swn.index = ix

    def iterkeys(self):
        return self.emap.iterkeys()

    def itervalues(self):
        return LRUCacheValuesIterator(self)

    def iteritems(self):
        return LRUCacheItemsIterator(self)

    def keys(self):
        return self.emap.keys()

    def values(self):
        return list(self.itervalues())

    def items(self):
        return list(self.iteritems())

    def __iter__(self):
        return self.iterkeys()

    def __setitem__(self, key, val):
        if key in self.emap:
            node = self.emap[key]
            node.value = val
            self.decrease(node)
        elif len(self.pqueue) >= self.size:
            node = self.pqueue[0]
            oldkey = node.key
            oldval = node.value
            del self.emap[node.key]
            node.key = key
            node.value = val
            self.emap[key] = node
            self.decrease(node)
            if self.eviction_callback is not None:
                self.eviction_callback(oldkey, oldval)
        else:
            node = _node(self.next_prio, len(self.pqueue), key, val)
            self.emap[key] = node
            self.pqueue.append(node)
            self.next_prio += 1
            if self.next_prio > 0xFFFFFFF0:
                self.c_rehash()

    def __getitem__(self, key):
        if key not in self.emap:
            raise CacheMissError(key)
        else:
            node = self.emap[key]
            if self.touch_on_read:
                self.decrease(node)
            return node.value
    
    def __delitem__(self, key):
        if key not in self.emap:
            raise CacheMissError(key)
        else:
            node = self.emap[key]
            self.decrease(node)

            node2 = self.pqueue[-1]
            if node2 is not node:
                self.pqueue[node.index] = node2
                node2.index = node.index

            del self.emap[key]
            del self.pqueue[-1]

    def cas(self, key, oldvalue, newvalue):
        if key in self.emap:
            node = self.emap[key]
            if node.value is oldvalue:
                node.value = newvalue
                self.decrease(node)
            elif self.touch_on_read:
                self.decrease(node)

    def get(self, key, deflt = None):
        if key not in self.emap:
            return deflt
        else:
            node = self.emap[key]
            self.decrease(node)
            return node.value
    
    def pop(self, key, deflt = CacheMissError):
        if key not in self.emap:
            if deflt is CacheMissError:
                raise CacheMissError(key)
            else:
                rv = deflt
        else:
            rv = self[key]
            del self[key]
        return rv

    def setdefault(self, key, deflt = None):
        if key not in self.emap:
            self[key] = deflt
            return deflt
        else:
            node = self.emap[key]
            self.decrease(node)
            return node.value

    def update(self, iterOrDict):
        if isinstance(iterOrDict, dict) or isinstance(iterOrDict, LRUCache):
            for k,v in iterOrDict.iteritems():
                self[k] = v
        else:
            for k,v in iterOrDict:
                self[k] = v

    def clear(self):
        self.pqueue = []
        self.emap = {}
        self.next_prio = 0
    
    def defrag(self):
        self.pqueue = list(self.pqueue)
        self.emap = self.emap.copy()

    def __repr__(self):
        return "<LRUCache (%d elements, %d max)>" % (len(self), self.size)


class LRUCacheItemsIterator(object):
    __slots__ = ('cache', 'pos')

    def __init__(self, cache):
        self.cache = cache
        self.pos = 0
    def __next__(self):
        if self.pos >= len(self.cache.pqueue):
            raise StopIteration
        else:
            node = self.cache.pqueue[self.pos]
            self.pos += 1
            return ( node.key, node.value )
    def __iter__(self):
        return self
        
    next = __next__

class LRUCacheValuesIterator(object):
    __slots__ = ('cache', 'pos')

    def __init__(self, cache):
        self.cache = cache
        self.pos = 0
    def __next__(self):
        if self.pos >= len(self.cache.pqueue):
            raise StopIteration
        else:
            node = self.cache.pqueue[self.pos]
            self.pos += 1
            return node.value
    def __iter__(self):
        return self

    next = __next__
