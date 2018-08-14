# -*- coding: utf-8 -*-
import time
import threading
import weakref

from . import base

try:
    from chorde import lrucache
except ImportError:
    from chorde import pylrucache as lrucache
Cache = lrucache.LRUCache
CacheMissError = base.CacheMissError
CacheIsThreadsafe = lrucache.IsThreadsafe

if not CacheIsThreadsafe:

    import warnings
    warnings.warn("LRUCache extension module not built in, "
        "using pure-python version which is not atomic and requires"
        "explicit synchronization. Decreased performance will be noticeable")
    del warnings

try:
    from chorde import cuckoocache
except ImportError:
    from chorde import pycuckoocache as cuckoocache
CuckooCache = cuckoocache.LazyCuckooCache
assert issubclass(cuckoocache.CacheMissError, CacheMissError)

if CacheIsThreadsafe and not cuckoocache.IsThreadsafe:
    import warnings
    warnings.warn("CuckooCache extension module not built in, "
        "but LRUCache module was built. InprocCacheClient will be assumed "
        "thread-safe, you will need to wrap it in a synchronized adapter "
        "to be used with CuckooCache!")

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

def cachePurge(timeout = 0, sleeptime = None):
    with _caches_mutex:
        caches = _caches.keys()

    for cache in caches:
        if sleeptime is not None:
            time.sleep(sleeptime)
        cache.purge(timeout)

def cacheClear():
    """
    Clear all @cached caches (use with moderation)

    >>> from chorde.decorators import cached
    >>> from chorde.clients.inproc import cacheClear, InprocCacheClient
    >>> import itertools
    >>> random = itertools.cycle(iter((0.9560342718892494, 0.9478274870593494, 2, 3, 4, 5, 6, 7, 8))).next
    >>> @cached(InprocCacheClient(1000), ttl = 6000)
    ... def f():
    ...     return random()
    ...
    >>> @cached(InprocCacheClient(1000), ttl = 6000)
    ... def g():
    ...     return random()
    ...
    >>> f()
    0.9560342718892494
    >>> g()
    0.9478274870593494
    >>> f()
    0.9560342718892494
    >>> g()
    0.9478274870593494
    >>> cacheClear()
    >>> f()
    2
    >>> g()
    3
    >>> f()
    2
    >>> g()
    3
    """

    with _caches_mutex:
        caches = _caches.keys()

    for cache in caches:
        cache.clear()

class CacheJanitorThread(threading.Thread):

    def __init__(self, sleep_interval, purge_timeout = 0):
        threading.Thread.__init__(self)
        self.sleep_interval = sleep_interval
        self.purge_timeout = purge_timeout
        self.logger = None
        self.setDaemon(True)

    def run(self):
        global cachePurge

        while True:
            time.sleep(self.sleep_interval)
            try:
                cachePurge(self.purge_timeout, sleeptime = 0.01)
            except:
                if self.logger is None:
                    pass
                else:
                    self.logger.error("Exception during cache purge", exc_info = True)

def startCacheJanitorThread(sleep_interval=3600, purge_timeout=0):
    thread = CacheJanitorThread(sleep_interval, purge_timeout)
    thread.start()
    return thread

class InprocCacheClient(base.BaseCacheClient):
    def __init__(self, size, store_class = None, store_args = (), store_kwargs = {}):
        if store_class is None:
            store_class = Cache
        self.store = store_class(size, *store_args, **store_kwargs)
        self.purgecount = 0
        _register_inproc(self)

    @property
    def async(self):
        return False

    @property
    def capacity(self):
        return self.store.size

    @property
    def usage(self):
        return len(self.store)

    def put(self, key, value, ttl, time = time.time):
        self.store[key] = (value, time() + ttl)

    def renew(self, key, ttl, baseNONE = base.NONE, time = time.time):
        origvalue = self.store.get(key, baseNONE)
        if origvalue is not baseNONE:
            value, kttl = origvalue
            ttl += time()
            if kttl < ttl:
                self.store.cas(key, origvalue, (value, ttl))

    def add(self, key, value, ttl):
        now = time.time()
        new_entry = (value, now + ttl)
        cur_entry = self.store.setdefault(key, new_entry)
        if cur_entry is new_entry:
            return True
        else:
            # Check TTL
            if cur_entry[1] < now:
                self.store[key] = new_entry
                return True
            else:
                return False

    def delete(self, key):
        try:
            del self.store[key]
        except CacheMissError:
            pass

    def expire(self, key):
        now = time.time()
        store = self.store
        try:
            value, ttl = store.pop(key)
            store.setdefault(key, (value, now))
        except CacheMissError:
            pass

    def getTtl(self, key, default = base.NONE, ttl_skip=None,
            promote_callback=None, baseNONE = base.NONE, time=time.time):
        rv = self.store.get(key, baseNONE)
        if rv is not baseNONE:
            rv, ttl = rv
            ttl = ttl - time()
            return rv, ttl
        elif default is baseNONE:
            raise CacheMissError, key
        else:
            return default, -1

    def clear(self):
        self.store.clear()

    def purge(self, timeout = 0):
        deletions = []
        retentions = []
        cache = self.store
        curtime = time.time() - timeout
        try:
            deletions_append = deletions.append
            for k, (v, timestamp) in cache.iteritems():
                if timestamp < curtime:
                    deletions_append(k)
        except RuntimeError:
            # Store changed, try with a snapshot
            # Only needed if the store isn't the LRU implementation
            # which should only happen in really crippled systems
            del deletions[:]
            cache_get = cache.get
            deletions_append = deletions.append
            for k in cache.keys():
                v = cache_get(v)
                if v is not None:
                    v, timestamp = v
                    if timestamp < curtime:
                        deletions_append(k)
        retentions_append = retentions.append
        cache_pop = cache.pop
        for k in deletions:
            # keep them alive so that no finalizations occur within the mutex's scope
            # (when wrapped inside a ReadWriteSyncAdapter), otherwise weird deadlocks
            # could arise.
            retentions_append(cache_pop(k, None))
        self.purgecount += 1
        if self.purgecount > 16:
            self.store.defrag()
            self.purgecount = 0

        # Returning them makes them live at least until the sync-wrapped method ends
        return retentions

    def contains(self, key, ttl = None, baseNONE = base.NONE, time = time.time):
        if key in self.store:
            if ttl is None:
                ttl = 0

            rv = self.store.get(key, baseNONE)
            if rv is not baseNONE:
                store_ttl = rv[1] - time()
                return store_ttl > ttl
            else:
                return False
        else:
            return False

if not CacheIsThreadsafe:
    InprocCacheClient_ = InprocCacheClient
    def InprocCacheClient(*p, **kw):
        return base.ReadWriteSyncAdapter(InprocCacheClient_(*p, **kw))

