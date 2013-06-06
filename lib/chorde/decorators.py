# -*- coding: utf-8 -*-
from functools import wraps, partial
import weakref
import md5
import time

from .clients import base, async, tiered

try:
    from .mq import coherence
    from .clients import coherent
    no_coherence = False
except ImportError:
    no_coherence = True

from .clients.base import CacheMissError

class NO_NAMESPACE:
    pass

class _NONE:
    pass

try:
    import multiprocessing
except ImportError:
    class multiprocessing:
        @staticmethod
        def cpu_count():
            return 1

def _make_namespace(f):
    fname = getattr(f, '__name__', None)
    if fname is None:
        fname = getattr(f, 'func_name', None)
    if fname is None:
        # FTW
        return repr(f)
    
    mname = getattr(f, '__module__', '')

    fcode = getattr(f, '__code__', None)
    if fcode is None:
        fcode = getattr(f, 'func_code', None)
    if fcode is not None:
        fpath = '%s:%d' % (fcode.co_filename, fcode.co_firstlineno)
    else:
        fpath = ''
    
    try:
        body_digest = md5.md5(fpath)
        if fcode:
            body_digest.update(getattr(fcode, 'co_code', ''))
        return "%s.%s#%s" % (mname,fname,body_digest.digest().encode("base64").strip("=\n"))
    except:
        return repr(f)

def _simple_put_deferred(client, f, key, ttl, *p, **kw):
    return client.put(key, async.Defer(f, *p, **kw), ttl)

def _coherent_put_deferred(shared, async_ttl, client, f, key, ttl, *p, **kw):
    return client.put_coherently(key, ttl, 
        lambda : not shared.contains(key, async_ttl), 
        f, *p, **kw)

class CacheStats(object):
    __slots__ = (
        'hits', 'misses', 'errors',
        'sync_misses', 'sync_errors',
        'min_miss_time', 'max_miss_time', 'sum_miss_time', 'sum_miss_time_sq',
        'miss_time_histogram', 'miss_time_histogram_bins', 'miss_time_histogram_max',
    )

    def __init__(self):
        self.clear()
        self.set_histogram_bins(0, None)

    def clear(self):
        self.hits = self.misses = self.errors = self.sync_misses = self.sync_errors = 0
        self.max_miss_time = self.sum_miss_time = self.sum_miss_time_sq = 0
        self.min_miss_time = None

    def set_histogram_bins(self, bins, maxtime):
        if bins <= 0:
            self.miss_time_histogram = self.miss_time_histogram_bins = self.miss_time_histogram_max = None
        else:
            self.miss_time_histogram = [0] * bins
            self.miss_time_histogram_bins = bins
            self.miss_time_histogram_max = maxtime

    def add_histo(self, time, min=min):
        if self.miss_time_histogram:
            hmax = self.miss_time_histogram_max
            hbins = self.miss_time_histogram_bins
            hbin = min(hbins-1, min(hmax, time) * hbins / hmax)
            self.miss_time_histogram[hbin] += 1

def cached(client, ttl,
        key = lambda *p, **kw:(p,frozenset(kw.items()) or ()),
        namespace = None,
        coherence = None,
        value_serialization_function = None,
        value_deserialization_function = None,
        async_writer_queue_size = None, 
        async_writer_workers = None,
        async_ttl = None,
        initialize = None,
        decorate = None,
        timings = True,
        _put_deferred = _simple_put_deferred ):
    """
    This decorator provides cacheability to suitable functions.

    To be considered suitable, the values received on parameters must be immutable types (otherwise caching will
    be unreliable).

    Caches are thread-safe only if the provided clients are thread-safe, no additional safety is provided. If you
    have a thread-unsafe client you want to make safe, use a (ReadWrite)SyncAdapter. Since synchronization adapters 
    only apply to store manipuation functions, and not the wrapped function, deadlocks cannot occur.

    The decorated function will provide additional behavior through attributes:
        client: the backing cache client. The provided client is never used as-is, and instead is wrapped in a 
            NamespaceWrapper. This is it.

        ttl: The configured TTL

        clear(): forget all cached values. Since the client might be shared, it will only increase an internal
            revision mark used to decorate keys, so the cache will not be immediately purged. For that, use
            client.clear() (but beware that it will also clear other caches sharing the same client).

        invalidate(...): mimicking the underlying function's signature, it will, instead of invoking the function,
            invalidate cached entries sharing the call's key.

        put(_cache_put, ...): mimicking the underlying function's signature after the first positional argument,
            except for one keyword argument _cache_put, it will forcibly set the cached value for that key to
            be what's supplied in _cache_put. Although there will be no invocation of the target function,
            the write will be synchronous unless the underlying cache client is async, and for external caches
            this might still mean a significant delay.

        refresh(...): mimicking the underlying function's signature, it will forcefully invoke the function,
            regardless of cache status, and refresh the cache with the returned value.

        peek(...): mimicking the underlying function's signature, it will query the cache without ever invoking
            the underlying function. If the cache doesn't contain the key, a CacheMissError will be raised.

        lazy(...): mimicking the underlying function's signature, it will behave just like a cached function call,
            except that if there is no value, instead of waiting for one to be computed, it will just raise
            a CacheMissError. If the access is async, it will start a background computation. Otherwise, it will
            behave just as peek.

        async(): if the underlying client is async, it will return the decorated function (self). Otherwise, it will
            be another decorated function, created on demand, backed by the same client wrapped in an async adapter.
            As such, it can be used to perform asynchronous operations on an otherwise synchronous function.

        stats: cache statistics, containing:
            hits - number of cache hits
            misses - number of cache misses
            errors - number of exceptions caught
            sync_misses - number of synchronous (blocking) misses
            sync_errors - number of synchronous exceptions (propagated to caller)

            min_miss_time - minimum time spent computing a miss
            max_miss_time - maximum time spent computing a miss
            sum_miss_time - total time spent computing a miss (divide by misses and get an average)
            sum_miss_time_sq - sum of squared times spent computing a miss (to compute standard deviation)

            miss_time_histogram - histogram of times spent computing misses, computed only if histogram
                bins and limits are set.
            miss_time_histogram_bins - number of bins configured
            miss_time_histogram_max - maximum histogram time configured

            reset(): clear all statistics
            set_histogram_bins(bins, max): configure histogram collection to use "bins" bins spanning
                from 0 to max. If bins is set to 0, max is ignored, and histogram collection is disabled

            All values are approximate, as no synchroniation is attempted while updating.

            sync_misses and sync_errors are caller-visible misses or exceptions. The difference with 
            misses and errors respectively are the number of caller-invisible misses or errors.

    Params
        client: the cache store client to be used

        ttl: the time, in seconds, during which values remain valid.

        key: (optional) A key derivation function, that will take the same arguments as the underlying function, 
            and should return a key suitable to the client. If not provided, a default implementation that will
            usually work for most primitive argument types will be used.

        namespace: (optional) If provided, the namespace used to identify cache entries will be the one provided.
            If not, a default one will be derived out of the function's module and name, which may differ between
            platforms, so you'll want to provide a stable one for shared caches. If NO_NAMEPSACE is given,
            no namespace decoration will be applied. Specify if somehow collisions are certain not to occur.

        value_serialization_function: (optional) If provided, values will not be stored directly into the cache,
            but the result of applying this function to them. Use if the cache is remote and does not natively
            support the results given by the underlying function, or if stripping of runtime-specific data is
            required.

        value_deserialization_function: (optional) Counterpart to value_serialization_function, it will be applied
            on values coming from the cache, before returning them as cached function calls.

        async_writer_queue_size: (optional) Writer queue size used for the async() client. Default is 100.

        async_writer_workers: (optional) Number of async workers for the async() client. 
            Default is multiprocessing.cpu_count

        async_ttl: (optional) The TTL at which an async refresh starts. For example, async_ttl=1 means to start
            a refresh just 1 second before the entry expires. It must be smaller than ttl. Default is half the TTL.
            If negative, it means ttl - async_ttl, which reverses the logic to mean "async_ttl" seconds after
            creation of the cache entry.

        initialize: (optional) A callable hook to be called right before all accesses. It should initialize whatever
            is needed initialized (like daemon threads), and only once (it should be a no-op after it's called once)

        decorate: (optional) A decorator to apply to all call-like decorated functions. Since @cached creates many
            variants of the function, this is a convenience over manually decorating all variants.

        timings: (optional) Whether to gather timing statistics. If true, misses will be timed, and timing data
            will be included in the stats attribute. It does imply some overhead. Default is True.
    """
    if value_serialization_function or value_deserialization_function:
        client = base.DecoratedWrapper(client,
            value_decorator = value_serialization_function,
            value_undecorator = value_deserialization_function )
    if namespace is not None and namespace is not NO_NAMESPACE:
        client = base.NamespaceWrapper(namespace, client)

    if not client.async:
        if async_writer_queue_size is None:
            async_writer_queue_size = 100
        if async_writer_workers is None:
            async_writer_workers = multiprocessing.cpu_count()

    if async_ttl is None:
        async_ttl = ttl / 2
    elif async_ttl < 0:
        async_ttl = ttl - async_ttl

    if _put_deferred is None:
        _put_deferred = _simple_put_deferred

    def decor(f):
        if namespace is None:
            nclient = base.NamespaceWrapper(_make_namespace(f), client)
        else:
            nclient = client

        stats = CacheStats()

        # Wrap and track misses and timings
        if timings:
            of = f
            @wraps(f)
            def af(*p, **kw):
                stats.misses += 1
                try:
                    t0 = time.time()
                    rv = of(*p, **kw)
                    t1 = time.time()
                    t = t1-t0
                    try:
                        stats.max_miss_time = max(stats.max_miss_time, t)
                        stats.min_miss_time = t if stats.min_miss_time is None else min(stats.min_miss_time, t)
                        stats.sum_miss_time += t
                        stats.sum_miss_time_sq += t*t
                        if stats.miss_time_histogram:
                            stats.add_histo(t)
                    except:
                        # Ignore stats collection exceptions. 
                        # Quite possible since there is no thread synchronization.
                        pass
                    return rv
                except:
                    stats.errors += 1
                    raise
            @wraps(f)
            def f(*p, **kw):
                stats.sync_misses += 1
                return af(*p, **kw)
        else:
            of = f
            @wraps(f)
            def af(*p, **kw):
                stats.misses += 1
                try:
                    return of(*p, **kw)
                except:
                    stats.errors += 1
                    raise
            @wraps(f)
            def f(*p, **kw):
                stats.sync_misses += 1
                return af(*p, **kw)

        @wraps(f)
        def cached_f(*p, **kw):
            if initialize is not None:
                initialize()
            try:
                callkey = key(*p, **kw)
            except:
                # Bummer
                stats.errors += 1
                return f(*p, **kw)
            
            try:
                rv = nclient.get(callkey)
                stats.hits += 1
            except CacheMissError:
                rv = f(*p, **kw)
                nclient.put(callkey, rv, ttl)
            return rv
        if decorate is not None:
            cached_f = decorate(cached_f)
        
        @wraps(f)
        def async_cached_f(*p, **kw):
            if initialize is not None:
                initialize()
            try:
                callkey = key(*p, **kw)
            except:
                # Bummer
                stats.errors += 1
                return f(*p, **kw)

            client = aclient[0]
            rv, rvttl = client.getTtl(callkey, _NONE)

            if (rv is _NONE or rvttl < async_ttl) and not client.contains(callkey, async_ttl):
                # Launch background update
                _put_deferred(client, af, callkey, ttl, *p, **kw)
            elif rv is not _NONE:
                stats.hits += 1

            if rv is _NONE:
                # Must wait for it
                client.wait(callkey)
                rv, rvttl = client.getTtl(callkey, _NONE)
                if rv is _NONE or rvttl < async_ttl:
                    # FUUUUU
                    rv = f(*p, **kw)
                else:
                    stats.sync_misses += 1
                    stats.misses += 1

            return rv
        if decorate is not None:
            async_cached_f = decorate(async_cached_f)
        
        @wraps(f)
        def lazy_cached_f(*p, **kw):
            if initialize is not None:
                initialize()
            try:
                callkey = key(*p, **kw)
            except:
                # Bummer
                stats.errors += 1
                raise CacheMissError
            
            rv = nclient.get(callkey)
            stats.hits += 1
            return rv
        if decorate is not None:
            lazy_cached_f = decorate(lazy_cached_f)
        
        @wraps(f)
        def invalidate_f(*p, **kw):
            if initialize is not None:
                initialize()
            try:
                callkey = key(*p, **kw)
            except:
                stats.errors += 1
                return
            nclient.delete(callkey)
        if decorate is not None:
            invalidate_f = decorate(invalidate_f)
        
        @wraps(f)
        def put_f(*p, **kw):
            value = kw.pop('_cache_put')
            if initialize is not None:
                initialize()
            try:
                callkey = key(*p, **kw)
            except:
                stats.errors += 1
                return
            nclient.put(callkey, value, ttl)
        if decorate is not None:
            put_f = decorate(put_f)
        
        @wraps(f)
        def async_put_f(*p, **kw):
            value = kw.pop('_cache_put')
            if initialize is not None:
                initialize()
            try:
                callkey = key(*p, **kw)
            except:
                stats.errors += 1
                return
            aclient[0].put(callkey, value, ttl)
        if decorate is not None:
            async_put_f = decorate(async_put_f)
        
        @wraps(f)
        def async_lazy_cached_f(*p, **kw):
            if initialize is not None:
                initialize()
            try:
                callkey = key(*p, **kw)
            except:
                # Bummer
                stats.errors += 1
                raise CacheMissError

            client = aclient[0]
            rv, rvttl = client.getTtl(callkey, _NONE)
            
            if (rv is _NONE or rvttl < async_ttl) and not client.contains(callkey, async_ttl):
                _put_deferred(client, af, callkey, ttl, *p, **kw)
            elif rv is not _NONE:
                stats.hits += 1

            if rv is _NONE:
                raise CacheMissError, callkey
            else:
                return rv
        if decorate is not None:
            async_lazy_cached_f = decorate(async_lazy_cached_f)

        @wraps(f)
        def refresh_f(*p, **kw):
            if initialize is not None:
                initialize()
            try:
                callkey = key(*p, **kw)
            except:
                # Bummer
                stats.errors += 1
                return

            rv = f(*p, **kw)
            nclient.put(callkey, rv, ttl)
            return rv
        if decorate is not None:
            refresh_f = decorate(refresh_f)

        @wraps(f)
        def async_refresh_f(*p, **kw):
            if initialize is not None:
                initialize()
            try:
                callkey = key(*p, **kw)
            except:
                # Bummer
                stats.errors += 1
                return

            client = aclient[0]
            if not client.contains(callkey, 0):
                _put_deferred(client, af, callkey, ttl, *p, **kw)
        if decorate is not None:
            async_refresh_f = decorate(async_refresh_f)

        if client.async:
            cached_f = async_cached_f
            lazy_cached_f = async_lazy_cached_f
        else:
            aclient = []
        
        if not client.async:
            def async_f():
                if initialize is not None:
                    initialize()
                if not aclient:
                    # atomic
                    aclient[:] = [async.AsyncWriteCacheClient(nclient, 
                        async_writer_queue_size, 
                        async_writer_workers)]
                    async_cached_f.client = aclient[0]
                return async_cached_f
            async_cached_f.clear = nclient.clear
            async_cached_f.client = None
            async_cached_f.async = weakref.ref(async_cached_f)
            async_cached_f.lazy = async_lazy_cached_f
            async_cached_f.refresh = async_refresh_f
            async_cached_f.peek = lazy_cached_f
            async_cached_f.invalidate = invalidate_f
            async_cached_f.put = async_put_f
            async_cached_f.ttl = async_ttl
            async_cached_f.stats = stats
            cached_f.async = async_f
            cached_f.lazy = lazy_cached_f
            cached_f.refresh = refresh_f
            cached_f.peek = lazy_cached_f
            cached_f.invalidate = invalidate_f
            cached_f.put = put_f
        else:
            aclient = [nclient]
            cached_f.async = weakref.ref(cached_f)
            cached_f.lazy = async_lazy_cached_f
            cached_f.refresh = async_refresh_f
            cached_f.peek = lazy_cached_f
            cached_f.invalidate = invalidate_f
            cached_f.put = async_put_f
        
        cached_f.clear = nclient.clear
        cached_f.client = nclient
        cached_f.ttl = ttl
        cached_f.stats = stats
        
        return cached_f
    return decor

if not no_coherence:
    
    def coherent_cached(private, shared, ipsub, ttl,
            key = lambda *p, **kw:(p,frozenset(kw.items()) or ()),
            tiered_ = None,
            namespace = None,
            coherence_namespace = None,
            coherence_encoding = 'pyobj',
            coherence_timeout = None,
            value_serialization_function = None,
            value_deserialization_function = None,
            async_writer_queue_size = None, 
            async_writer_workers = None,
            async_ttl = None,
            initialize = None,
            decorate = None,
            tiered_opts = None,
            **coherence_kwargs ):
        """
        This decorator provides cacheability to suitable functions, in a way that maintains coherency across
        multiple compute nodes.
    
        For suitability considerations and common parameters, refer to cached. The following describes the
        aspects specific to the coherent version.
    
        The decorated function will provide additional behavior through attributes:
            coherence: the coherence manager created for this purpse
    
            ipsub: the given IPSub channel
    
        Params
            ipsub: An IPSub channel that will be used to publish and subscribe to update events.
    
            private: The private (local) cache client, the one that needs coherence.
    
            shared: The shared cache client, that reflects changes made by other nodes.
    
            tiered_: (optional) A client that queries both, private and shared. By default, a TieredInclusiveClient
                is created with private and shared as first and second tier, which should be the most common case. 
                The private client will be wrapped in an async wrapper if not async already, to be able to execute
                the coherence protocol asynchronously. This should be adequate for most cases, but in some, 
                it may be beneficial to provide a custom client.
    
            tiered_opts: (optional) When using the default-constructed tiered client, you can pass additional (keyword)
                arguments here.
    
            coherence_namespace: (optional) There is usually no need to specify this value, the namespace in use 
                by caching will be used for messaging as well, or if caching uses NO_NAMESPACE, the default that
                would be used instead. However, for really high-volume channels, sometimes it is beneficial to pick
                a more compact namespace (an id formatted with struct.pack for example).
    
            coherence_encoding: (optional) Keys will have to be transmitted accross the channel, and this specifies
                the encoding that will be used. The default 'pyobj' should work most of the time, but it has to 
                be initialized, and 'json' or others could be more compact, depending on keys.
                (see CoherenceManager for details on encodings)
    
            coherence_timeout: (optional) Time (in ms) of peer silence that will be considered abnormal. Default
                is 2000ms, which is sensible given the IPSub protocol. You may want to increase it if node load
                creates longer hiccups.
    
            Any extra argument are passed verbatim to CoherenceManager's constructor.
        """
        if async_ttl is None:
            async_ttl = ttl / 2
        
        if not private.async:
            if async_writer_queue_size is None:
                async_writer_queue_size = 100
            if async_writer_workers is None:
                async_writer_workers = multiprocessing.cpu_count()
        
        def decor(f):
            if coherence_namespace is None:
                _coherence_namespace = _make_namespace(f)
            else:
                _coherence_namespace = coherence_namespace
    
            if namespace is None:
                _namespace = _make_namespace(f)
            else:
                _namespace = namespace
    
            if not private.async:
                nprivate = async.AsyncWriteCacheClient(private, 
                    async_writer_queue_size, 
                    async_writer_workers)
            else:
                nprivate = private
    
            if tiered_ is None:
                ntiered = tiered.TieredInclusiveClient(nprivate, shared, **(tiered_opts or {}))
            else:
                ntiered = tiered_
    
            if _namespace is not NO_NAMESPACE:
                ntiered = base.NamespaceWrapper(_namespace, ntiered)
                nprivate = base.NamespaceMirrorWrapper(ntiered, nprivate)
                nshared = base.NamespaceMirrorWrapper(ntiered, shared)
            else:
                nshared = shared
    
            coherence_manager = coherence.CoherenceManager(
                _coherence_namespace, nprivate, nshared, ipsub,
                encoding = coherence_encoding,
                **coherence_kwargs)
    
            nclient = coherent.CoherentWrapperClient(ntiered, coherence_manager, coherence_timeout)
    
            rv = cached(nclient, ttl,
                namespace = NO_NAMESPACE, # Already covered
                value_serialization_function = value_serialization_function,
                value_deserialization_function = value_deserialization_function,
                async_writer_queue_size = async_writer_queue_size, 
                async_writer_workers = async_writer_workers,
                async_ttl = async_ttl,
                initialize = initialize,
                decorate = decorate,
                _put_deferred = partial(_coherent_put_deferred, nshared, async_ttl) )(f)
            rv.coherence = coherence_manager
            rv.ipsub = ipsub
            return rv
        return decor
