Dependencies:
=============

* numpy *(for optimizations in FilesCacheClient)*
* python-memcached *(for MemcachedClient)*
* pyzmq < 14 *(for coherence support)*
* dnspython  *(for dynamic dns-based load-balancing of MemcachedClient)*
* cython or pyrex *(for optimizations in InprocCacheClient)*

Basic Usage:
============

The simplest way to use is to create one of the supported cache clients,
and use it directly, like

.. code:: python

	from chorde.clients.inproc import InprocCacheClient
	from chorde import CacheMissError
	c = InprocCacheClient(200)
	c.put(3, 10, 300) # put value 10 on key 3, TTL 5min
	assert 10 == c.get(3)
	try:
		c.get(5)
	except CacheMissError:
		print "miss"

This creates an In-process LRU cache. The in-process part indicates that it
is process-private, and not shared with other processes.

The most straightforward way to get a shared cache, is to use a memcache:

.. code:: python

	from chorde.clients.memcached import MemcachedClient
	from chorde import CacheMissError
	c = MemcachedClient(["localhost:11211"], checksum_key = "testing")
	c.put(3, 10, 300)
	assert 10 == c.get(3)
	try:
		c.get(5)
	except CacheMissError:
		print "miss"

The MemcachedClient is used just like any other client, only it talks to, in this
example, a local memcached listening on localhost port 11211. Multiple clients
can be given, and a consistent hash on the key will be used to spread the load
among them.

If a hostname is given, and the hostname points to multiple IP addresses, the
same effect will be obtained, and the distribution will be dynamically updated
according to the TTL specified on the DNS entry. This, for example, makes the
client work seamlessly with Amazon ElastiCache's "configuration endpoint", 
which is a DNS entry that points to one of the cache nodes. But it only works
like that with single-node clusters. For multi-node clusters, use
``chorde.clients.elasticache.ElastiCacheClient``, which goes a step further
and queries this configuration endpoint for all the other nodes.

Do beware that the key used against the memcached isn't the key given, since
the client supports any hashable object as key, whereas memcached only supports
a subset of string keys. MemcachedClient has none of memcached's limitations
with regards to key format and/or length and value size. It works around
memcaches limitations, by constructing a derived key that is suitable for
memcached. However, there is overhead in providing huge keys or values, so it's generally
good advise to avoid them anyway.

Values in the MemcachedClient are plickled, compressed and signed with the
checksum key, so it is relatively safe from both malicious code injection through
pickle, and transmission errors causing cPickle to dump core (which does happen when
it is fed unchecked data).

In case the compression becomes a bottleneck, which shouldn't be a problem unless
it is a high-traffic cache with rarely compressible values, one can disable it.
Check MemcachedClient's documentation for more details.

Multilevel caches
=================

A common approach when dealing with remote caches, like the above example using
memcached, is to have at least two levels: the memcached itself, and an in-process
small cache to avoid having to talk to the memcached all the time.

This can be done straightforwardly with the tiered clients:

.. code:: python

	from chorde.clients.memcached import MemcachedClient
	from chorde.clients.inproc import InprocCacheClient
	from chorde.clients.tiered import TieredInclusiveClient
	from chorde import CacheMissError
	l1 = InprocCacheClient(10)
	l2 = MemcachedClient(["localhost:11211"], checksum_key="test")
	c = TieredInclusiveClient(l1,l2)
        c.put(3, 10, 300)
        assert 10 == c.get(3)
        try:
                c.get(5)
        except CacheMissError:
                print "miss"

Here we build an *inclusive* tiered client, in which elements on higher levels are 
promoted into the lower levels by copying, rather than swapping. This means there
is duplication among them, but this is usually best in cases like these, where the
upper levels are shared among processes.

An exclusive client isn't provided at this moment, since there is seldom any use 
for the exclusive pattern on these types of caches.

Decorators
==========

A more natural way to think about caching, is in that it's a decorator of plain functions.

Rather than explicitly putting and getting from caches, one can simply consider
caching as an optimization on an otherwise expensive function.

Decorators in chorde.decorators provide a huge amount of functionality and flexibility,
these examples cover only the most basic usage:

Assuming *c* is the client we want to use for caching,

.. code:: python

	from chorde.decorators import cached
	import random
	
	@cached(c, ttl=300, async_ttl=-60)
	def expensive_func(x):
		return x * random.random()

	print expensive_func(1)
	print expensive_func(1) # Should return the same
	print expensive_func.async()(1) # will refresh asynchronously every minute
	print expensive_func.future()(1).result() # same as before, but using the futures interface
	print expensive_func.peek(1) # just check the cache
	print expensive_func.put(1, _cache_put=5) # write an explicit value
	print expensive_func.async().lazy(1) # don't wait, raise CacheMissError if not available, compute in background
	print expensive_func.future().lazy(1).result() # same as before, but using the futures interface

There, the async_ttl means the minimum TTL that triggers
an asynchronous recomputation (you can use it to avoid ever having to wait on a recomputation).
The negative value makes it relative to the total TTL, so -60 always means recompute
every minute (60 seconds). The plain ttl is an absolute limit, no result older than
that will ever be returned.

The documentation on chorde.decorators.cached will have more to say about the ways of
invoking cached functions. 

In general, the terms are:

  * lazy: don't wait for computation, return a cached result or raise CacheMissError.
    When combined with async, it will compute in the background.
  * peek: don't compute. Similar to lazy, but it will never trigger a computation
  * async: expensive things (computation) happen on a background threadpool.
  * future: return futures rather than results, use the future to get notified of
    results when they're available. Actual cache access happens on a threadpool.
    A non-blocking way of calling.
  * refresh: immediately recompute the value.


Using decorators with tornado coroutines
----------------------------------------

The decorators' future() interface is especially suited for integration with other libraries that can talk to
futures. Chorde's futures, however, are not directly compatible with other libraries', but they can easily be
wrapped like so:

.. code:: python

	import tornado.web
	import tornado.gen
	from chorde.clients.async import makeFutureWrapper
	
	WF = makeFutureWrapper(tornado.web.Future)
	
	...
	
	@tornado.gen.coroutine
	def get(self):
		some_result = yield WF(some_func.future()(some_args))
