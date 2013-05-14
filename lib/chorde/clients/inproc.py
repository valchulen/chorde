# -*- coding: utf-8 -*-
import time
import threading
import weakref

from . import base

try:

    import lrucache
    Cache = lrucache.LRUCache
    CacheMissError = base.CacheMissError = lrucache.CacheMissError
    CacheIsThreadsafe = True

except:

    class Cache(dict):
        def __init__(self, size):
            self.size = size
        def __setitem__(self, key, value):
            if len(self) >= self.size:
                del self[iter(self).next()]
            super(self.__class__,self).__setitem__(key,value)
        def defrag(self):
            items = self.items()
            self.clear()
            self.update(items)
    CacheMissError = base.CacheMissError = KeyError
    CacheIsThreadsafe = False

_caches_mutex = threading.RLock()
_caches = weakref.WeakKeyDictionary()

def _register_inproc(cache):
    _caches[cache] = None

def cacheStats():
    global _caches
    global _caches_mutex

    with _caches_mutex:
        rv = {}
        for cache in _caches.iterkeys():
            fname = cache.func_name
            
            # Sometimes, functions are different but named the same. Usually
            # they're related, so we aggregate those stats.
            ocsize, oclen = rv.get(fname, (0,0))
            rv[fname] = ( cache.store.size + ocsize, len(cache.store)+oclen )
            
        return rv

def cachePurge():
    with _caches_mutex:
        caches = _caches.keys()
    
    for cache in caches:
        cache.purge()

def cacheClear():
    """
    Clear all @cached caches (use with moderation)

    >>> from decorators import cached, cacheClear
    >>> @cached(timeout=6000, maxcache=1000)
    ... def f():
    ...     import random
    ...     return random.random()
    ...
    >>> @cached(timeout=6000, maxcache=1000)
    ... def g():
    ...     import random
    ...     return random.random()
    ...
    >>> import random
    >>> random.seed(2)
    >>> f()
    0.95603427188924939
    >>> g()
    0.94782748705934938
    >>> f()
    0.95603427188924939
    >>> g()
    0.94782748705934938
    >>> cacheClear()
    >>> f()
    0.056551367726808688
    >>> g()
    0.084871995158921631
    >>> f()
    0.056551367726808688
    >>> g()
    0.084871995158921631
    """

    with _caches_mutex:
        caches = _caches.keys()
    
    for cache in caches:
        cache.clear()

class CacheJanitorThread(threading.Thread):

    def __init__(self, sleep_interval):
        threading.Thread.__init__(self)
        self.sleep_interval = sleep_interval
        self.setDaemon(True)
        
    def run(self):
        global cachePurge
        
        while True:
            time.sleep(self.sleep_interval)
            try:
                cachePurge()
            except:
                pass

def startCacheJanitorThread(sleep_interval=3600):
    thread = CacheJanitorThread(sleep_interval)
    thread.start()
    return thread

class InprocCacheClient(base.BaseCacheClient):
    def __init__(self, size):
        self.store = Cache(size)
        _register_inproc(self)

    @property
    def async(self):
        return False

    def put(self, key, value, ttl):
        self.store[key] = (value, time.time() + ttl)

    def delete(self, key):
        try:
            del self.store[key]
        except KeyError:
            pass

    def getTtl(self, key, default = base.NONE):
        rv = self.store.get(key, base.NONE)
        if rv is not base.NONE:
            rv, ttl = rv
            return rv, ttl - time.time()
        elif default is base.NONE:
            raise CacheMissError, key
        else:
            return default, -1

    def clear(self):
        self.store.clear()

    def purge(self, timeout):
        deletions = []
        retentions = []
        cache = self.store
        curtime = time.time() - timeout
        for k, (v, timestamp) in cache.iteritems():
            if timestamp < curtime:
                deletions.append(k)
        for k in deletions:
            # keep them alive so that no finalizations occur within the mutex's scope 
            # (when wrapped inside a ReadWriteSyncAdapter), otherwise weird deadlocks
            # could arise.
            retentions.append(cache[k])
            del cache[k]
        self.store.defrag()

        # Returning them makes them live at least until the sync-wrapped method ends
        return retentions

    def contains(self, key, ttl = None):
        if key in self.store:
            if ttl is None:
                ttl = 0

            rv = self.store.get(key, base.NONE)
            if rv is not base.NONE:
                store_ttl = rv[1] - time.time()
                return store_ttl > ttl
            else:
                return False
        else:
            return False

if not CacheIsThreadsafe:
    InprocCacheClient_ = InprocCacheClient
    def InprocCacheClient(*p, **kw):
        return base.ReadWriteSyncAdapter(InprocCacheClient_(*p, **kw))

