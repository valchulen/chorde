# -*- coding: utf-8 -*-
import time
import random
import unittest
import threading
import functools
from chorde.decorators import cached, coherent_cached, CacheMissError
from chorde.clients.inproc import InprocCacheClient
from chorde.clients.async import AsyncWriteCacheClient
from tests.coherence import skipIfUnsupported, ipsub, zmq


class DecoratorTestCase(unittest.TestCase):
    """Base test class"""

    def setUp(self):
        self.client = InprocCacheClient(100)

    def tearDown(self):
        self.client.clear()


class CachedDecoratorTest(DecoratorTestCase):
    """Basic tests for cached decorator"""
    def setUp(self):
        super(CachedDecoratorTest, self).setUp()
        self.decorator = functools.partial(cached, self.client)

    def test_cached(self):
        # Puts a random number in cache and checks the value in the client
        key = lambda: 'test_cached'
        @self.decorator(5, key=key)
        def get_random():
            return random.random()
        val = get_random()
        self.assertTrue(get_random.client.contains(key()))
        self.assertEquals(val, get_random())

    def test_get_ttl(self):
        # Puts a random number in cache and checks the value in the client
        key = lambda: 'test_cached'
        @self.decorator(5, key=key)
        def get_random():
            return random.random()
        val = get_random()
        self.assertTrue(get_random.client.contains(key()))
        self.assertEquals(val, get_random.get_ttl()[0])
        self.assertLess(0, get_random.get_ttl()[1])

    def test_ttl(self):
        # The client shouldn't contains the function key
        key = lambda: 'test_ttl'
        @self.decorator(1, key=key)
        def get_random():
            return random.random()
        get_random()
        time.sleep(1.1)
        self.assertFalse(get_random.client.contains(key()))

    def test_namespace(self):
        # If a namespace is provided, should create the key with that
        namespace = 'my_namespace'
        @self.decorator(5, namespace=namespace)
        def get_random():
            return random.random()
        self.assertEquals(get_random.client.namespace, namespace)

    def test_no_namespace(self):
        # Without namespace, should create one with the function name
        @self.decorator(5)
        def get_random():
            return random.random()
        namespace = get_random.client.namespace
        self.assertTrue(namespace.startswith(get_random.__module__))

    def test_serialization_function(self):
        # Should apply the a function the returned value
        f = lambda x: x+1
        val = []
        @self.decorator(5, value_serialization_function=f)
        def get_random():
            val[:] = [random.random()]
            return val[0]
        get_random()
        self.assertEquals(get_random(), f(val[0]))

    def test_deserialization_function(self):
        # Should apply the a function the returned value
        f = lambda x: x+3
        val = []
        @self.decorator(5, value_deserialization_function=f)
        def get_random():
            val[:] = [random.random()]
            return val[0]
        get_random()
        self.assertEquals(get_random(), f(val[0]))

    def test_renew(self):
        # Should add the renew time to the ttl
        key = lambda x=0: 'test_renew'
        renew = 1
        @self.decorator(ttl=2, async_ttl=-0.1, key=key, renew_time=renew)
        def get_random(delay):
            time.sleep(delay)
            return random.random()
        get_random(0)
        _, ttl1 = get_random.client.getTtl(key())
        time.sleep(0.2)
        get_random.async()(2)
        _, ttl2 = get_random.client.getTtl(key())
        diff = ttl2 - ttl1
        self.assertLess(abs(renew - diff), 0.2) # Almost equal

    def test_initialize(self):
        # Should call initialize once
        global count
        count = 0
        def init():
            global count
            count += 1
            return True
        @self.decorator(ttl=5, initialize=init)
        def test():
            return False
        test()
        self.assertEquals(count, 1)
        test()
        self.assertEquals(count, 1)
        del count

    def test_decorate(self):
        # Should apply a decorator to the decorated function
        def wrapped(f):
            return lambda: True
        @self.decorator(ttl=5, decorate=wrapped)
        def test():
            return False
        self.assertTrue(test())

    def test_uncached(self):
        # Should always call the function
        @self.decorator(ttl=10)
        def get_random():
            return random.random()
        val = get_random()
        self.assertNotEquals(val, get_random.uncached())

    def test_invalidate(self):
        # Should delete cache entry
        key = lambda: 'test_invalidate'
        @self.decorator(ttl=10, key=key)
        def get_random():
            return random.random()
        get_random()
        get_random.invalidate()
        time.sleep(0.1)
        self.assertFalse(get_random.client.contains(key()))

    def test_refresh(self):
        # Should refresh the cache value
        @self.decorator(ttl=10)
        def get_random():
            return random.random()
        val1 = get_random()
        val2 = get_random.refresh()
        self.assertNotEquals(val1, val2)

    def test_put(self):
        # Should change the cached value
        key = lambda: 'test_put'
        @self.decorator(ttl=10, key=key)
        def get_number():
            return 1
        val = get_number()
        get_number.put(_cache_put=val+2)
        self.assertEquals(get_number(), val+2)

    def test_peek_not_cached(self):
        # Should raise a CacheMissError
        @self.decorator(ttl=5)
        def not_cached():
            return random.random()
        self.assertRaises(CacheMissError, not_cached.peek)

    def test_peek_cached(self):
        # Shouldn't raise a CacheMissError
        @self.decorator(ttl=5)
        def get_random():
            return random.random()
        val = get_random()
        self.assertEquals(get_random.peek(), val)


class CachedDecoratorFutureTest(DecoratorTestCase):
    """Tests future functionality for cached decorator"""

    def test_future_invalidate(self):
        # Should delete cache entry
        key = lambda: 'test_invalidate'
        @cached(self.client, ttl=10, key=key)
        def get_random():
            return random.random()
        get_random()
        get_random.future().invalidate().result()
        time.sleep(0.1)
        self.assertFalse(get_random.client.contains(key()))

    def test_future_sync_check(self):
        # Should wait and return the value
        val = []
        @cached(self.client, ttl=5, future_sync_check=True)
        def get_random():
            time.sleep(0.1)
            val[:] = [random.random()]
            return val[0]
        r = get_random.future()().result()
        self.assertEquals(r, val[0])

    def test_future_no_sync_check(self):
        # Shouldn't wait for the value
        @cached(self.client, ttl=5, future_sync_check=False)
        def get_random():
            time.sleep(4)
            return random.random()
        f = get_random.future()()
        self.assertFalse(f.done())

    def test_future_sync_check_on_value(self):
        # Should return the value using on_value function with sync check
        val = []
        @cached(self.client, ttl=5, async_ttl=10, future_sync_check=True)
        def get_random():
            val[:] = [random.random()]
            return val[0]
        get_random()
        r = get_random.future()().result()
        self.assertEquals(r, val[0])

    def test_future_sync_check_value_loaded(self):
        # Future should return the value instantly
        @cached(self.client, ttl=5, future_sync_check=True)
        def get_random():
            return random.random()
        get_random()
        hits = get_random.stats.hits
        get_random.future()().result()
        self.assertEquals(get_random.stats.hits, hits+1)

    def test_future_on_value(self):
        # Should return the value using on_value function
        val = []
        @cached(self.client, ttl=5)
        def get_random():
            val[:] = [random.random()]
            return val[0]
        get_random()
        r = get_random.future()().result()
        self.assertEquals(r, val[0])
    
    def test_future_on_value_bad_key(self):
        # Should return the value using on_value function, even when given a bad callkey callable
        # To-do: validate bad key logged
        val = []
        @cached(self.client, ttl=5, key = lambda : None)
        def get_random(k):
            val[:] = [random.random()]
            return val[0]
        get_random(set())
        r = get_random.future()(set()).result()
        self.assertEquals(r, val[0])
    
    def test_future_lazy(self):
        # Should start calculating the value in background
        @cached(self.client, ttl=5)
        def get_number():
            return 8
        rv = get_number.future().lazy()
        self.assertRaises(CacheMissError, rv.result)
        time.sleep(0.1)
        rv = get_number.future().lazy()
        self.assertEquals(rv.result(), 8)

    def test_future_lazy_sync_check_value_loaded(self):
        # Future should return the value instantly
        @cached(self.client, ttl=5, async_ttl=10, future_sync_check=True)
        def get_random():
            return random.random()
        get_random()
        hits = get_random.stats.hits
        get_random.future().lazy().result()
        self.assertEquals(get_random.stats.hits, hits+1)

    def test_future_peek_uncached(self):
        # Should raise a CacheMissError
        @cached(self.client, ttl=5)
        def get_random():
            return random.random()
        self.assertRaises(CacheMissError, get_random.future().peek().result)

    def test_future_peek_cached_on_value(self):
        # Should call on_value function and return the value
        @cached(self.client, ttl=5)
        def get_random():
            return random.random()
        val = get_random()
        self.assertEquals(get_random.future().peek().result(), val)

    def test_future_peek_sync_check(self):
        # Should return the value directly from the client
        @cached(self.client, ttl=5, future_sync_check=True)
        def get_random():
            return random.random()
        val = get_random()
        self.assertEquals(get_random.future().peek().result(), val)

    def test_future_refresh(self):
        # Should refresh the cache value
        @cached(self.client, ttl=10)
        def get_random():
            return random.random()
        val1 = get_random()
        val2 = get_random.future().refresh().result()
        self.assertNotEquals(val1, val2)


class CachedDecoratorAsyncTest(DecoratorTestCase):
    """Tests async functionality for cached decorator"""

    def setUp(self):
        super(CachedDecoratorAsyncTest, self).setUp()
        self.client = AsyncWriteCacheClient(self.client, 100)

    def test_lazy_async(self):
        # Should raise a CacheMissError and start calculating the value
        key = lambda: 'test_lazy_async'
        @cached(self.client, ttl=10, key=key)
        def get_number():
            return 4
        get_number_async = get_number.async()
        self.assertRaises(CacheMissError, get_number_async.lazy)
        time.sleep(0.1)
        self.assertEquals(get_number_async.lazy(), 4)
            
    def test_recalculate_async_on_lower_ttl(self):
        # When the value is expired it's recalculated
        key = lambda: 'test_async_ttl'
        @cached(self.client, ttl=5, async_ttl=8, key=key)
        def get_random():
            return random.random()
        val = get_random.async()()
        self.assertNotEquals(val, get_random.async()())

    def test_cached_async(self):
        # Puts a random number in cache and checks the value in the client
        key = lambda: 'test_async_cached'
        @cached(self.client, ttl=5, key=key)
        def get_random():
            return random.random()
        self.assertEquals(get_random, get_random.async())
        val = get_random()
        self.assertEquals(val, get_random())
            
    def test_put_async(self):
        # Should change the cached value
        key = lambda: 'test_put_async'
        @cached(self.client, ttl=10, key=key)
        def get_number():
            return 1
        val = get_number()
        get_number.put(_cache_put=val+2)
        self.assertEquals(get_number(), val+2)

    def test_lazy_cached_async(self):
        # Should raise a CacheMissError and call the function in background
        key = lambda: 'test_async_lazy_cached'
        val = []
        @cached(self.client, ttl=5, key=key)
        def get_random():
            time.sleep(0.1)
            val[:] = [random.random()]
            return val[0]
        self.assertRaises(CacheMissError, get_random.async().lazy)
        time.sleep(0.2)
        self.assertEquals(val[0], get_random.async().lazy())
            
    def test_lazy_recheck_async(self):
        # Should touch the key with async_lazy_recheck
        key = lambda: 'test_async_lazy_recheck'
        @cached(self.client, ttl=5, key=key, async_lazy_recheck=True)
        def get_random():
            time.sleep(0.1)
            return random.random()
        self.assertRaises(CacheMissError, get_random.async().lazy)
        self.assertTrue(get_random.client.contains(key()))
        time.sleep(0.2)
            
    def test_refresh_async(self):
        # Should refresh the cache value
        @cached(self.client, ttl=10)
        def get_random():
            return random.random()
        val1 = get_random.async()
        val2 = get_random.async().refresh()
        self.assertNotEquals(val1, val2)

    def test_serialization_function(self):
        # Should apply the a function the returned value
        f = lambda x: x+1
        val = []
        @cached(self.client, 5, value_serialization_function=f)
        def get_random():
            val[:] = [random.random()]
            return val[0]
        get_random()
        self.assertEquals(get_random(), f(val[0]))

    def test_deserialization_function(self):
        # Should apply the a function the returned value
        f = lambda x: x+3
        val = []
        @cached(self.client, 5, value_deserialization_function=f)
        def get_random():
            val[:] = [random.random()]
            return val[0]
        get_random()
        self.assertEquals(get_random(), f(val[0]))


@skipIfUnsupported
class CoherentCachedDecoratorTest(CachedDecoratorTest):
    @classmethod
    def setUpClass(cls):
        ipsub.IPSub.register_default_pyobj()
        
        ctx = zmq.Context.instance()
        s1 = ctx.socket(zmq.REQ)
        s2 = ctx.socket(zmq.REQ)
        port1 = s1.bind_to_random_port("tcp://127.0.0.1")
        port2 = s2.bind_to_random_port("tcp://127.0.0.1")
        s1.close()
        s2.close()
        del s1,s2
        
        cls.ipsub = ipsub.IPSub([dict(rep="tcp://127.0.0.1:%d" % port1, 
            pub="tcp://127.0.0.1:%d" % port2)], ctx=ctx)
        cls.ipsub_thread = threading.Thread(target=cls.ipsub.run)
        cls.ipsub_thread.daemon = True

        cls.private = InprocCacheClient(100)
        cls.shared = InprocCacheClient(100)

        time.sleep(0.1)
        
        cls.ipsub_thread.start()
        
        time.sleep(0.1)

    def setUp(self):
        super(CoherentCachedDecoratorTest, self).setUp()
        self.decorator = functools.partial(coherent_cached, self.private, 
                self.shared, self.ipsub)

    @classmethod
    def tearDownClass(cls):
        cls.ipsub.terminate()
        cls.ipsub.wake()
        del cls.private, cls.shared
        cls.ipsub_thread.join(5000)
        del cls.ipsub, cls.ipsub_thread

    def test_serialization_function(self):
        # Should apply the a function the returned value
        f = lambda x: x+1
        val = []
        @cached(self.client, 5, value_serialization_function=f)
        def get_random():
            val[:] = [random.random()]
            return val[0]
        get_random()
        self.assertEquals(get_random(), f(val[0]))

    def test_deserialization_function(self):
        # Should apply the a function the returned value
        f = lambda x: x+3
        val = []
        @cached(self.client, 5, value_deserialization_function=f)
        def get_random():
            val[:] = [random.random()]
            return val[0]
        get_random()
        self.assertEquals(get_random(), f(val[0]))

    def test_namespace(self):
        # If a namespace is provided, should create the key with that
        namespace = 'my_namespace'
        @self.decorator(5, namespace=namespace)
        def get_random():
            return random.random()
        self.assertEquals(get_random.client.client.namespace, namespace)

    def test_no_namespace(self):
        # Without namespace, should create one with the function name
        @self.decorator(5)
        def get_random():
            return random.random()
        namespace = get_random.client.client.namespace
        self.assertTrue(namespace.startswith(get_random.__module__))

