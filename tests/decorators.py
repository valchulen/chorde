# -*- coding: utf-8 -*-
import time
import random
import unittest
from chorde.decorators import cached, CacheMissError
from chorde.clients.inproc import InprocCacheClient
from chorde.clients.async import AsyncWriteCacheClient

class DecoratorBaseTest(unittest.TestCase):
    """Base test class"""

    @classmethod
    def setUpClass(cls):
        cls.client = InprocCacheClient(100)

    def tearDown(self):
        self.client.clear()


class CachedDecoratorTest(DecoratorBaseTest):
    """Basic tests for cached decorator"""

    def test_cached(self):
        """Puts a random number in cache and checks the value in the client"""
        key = lambda: 'test_cached'
        @cached(self.client, 5, key=key)
        def get_random():
            return random.random()
        val = get_random()
        self.assertTrue(get_random.client.contains(key()))
        self.assertEquals(val, get_random())

    def test_ttl(self):
        """The client shouldn't contains the function key"""
        key = lambda: 'test_ttl'
        @cached(self.client, 1, key=key)
        def get_random():
            return random.random()
        get_random()
        time.sleep(1.1)
        self.assertFalse(get_random.client.contains(key()))

    def test_namespace(self):
        """If a namespace is provided, should create the key with that"""
        namespace = 'my_namespace'
        @cached(self.client, 5, namespace=namespace)
        def get_random():
            return random.random()
        self.assertEquals(get_random.client.namespace, namespace)

    def test_no_namespace(self):
        """Without namespace, should create one with the function name"""
        @cached(self.client, 5)
        def get_random():
            return random.random()
        namespace = get_random.client.namespace
        self.assertTrue(namespace.startswith(get_random.__module__))

    def test_serialization_function(self):
        """Should apply the a function the returned value"""
        f = lambda x: x+1
        @cached(self.client, 5, value_serialization_function=f)
        def get_random():
            return random.random()
        val = get_random()
        self.assertEquals(get_random(), val+1)

    def test_deserialization_function(self):
        """Should apply the a function the returned value"""
        f = lambda x: x+3
        @cached(self.client, 5, value_deserialization_function=f)
        def get_random():
            return random.random()
        val = get_random()
        self.assertEquals(get_random(), val+3)

    def test_renew(self):
        """Should add the renew time to the ttl"""
        key = lambda x=0: 'test_renew'
        renew = 1
        @cached(self.client, ttl=2, async_ttl=-0.1, key=key, renew_time=renew)
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
        """Should call initialize once"""
        global count
        count = 0
        def init():
            global count
            count += 1
            return True
        @cached(self.client, ttl=5, initialize=init)
        def test():
            return False
        test()
        self.assertEquals(count, 1)
        test()
        self.assertEquals(count, 1)
        del count

    def test_decorate(self):
        """Should apply a decorator to the decorated function"""
        def wrapped(f):
            return lambda: True
        @cached(self.client, ttl=5, decorate=wrapped)
        def test():
            return False
        self.assertTrue(test())

    def test_uncached(self):
        """Should always call the function"""
        @cached(self.client, ttl=10)
        def get_random():
            return random.random()
        val = get_random()
        self.assertNotEquals(val, get_random.uncached())

    def test_invalidate(self):
        """Should delete cache entry"""
        key = lambda: 'test_invalidate'
        @cached(self.client, ttl=10, key=key)
        def get_random():
            return random.random()
        get_random()
        get_random.invalidate()
        self.assertFalse(get_random.client.contains(key()))

    def test_refresh(self):
        """Should refresh the cache value"""
        @cached(self.client, ttl=10)
        def get_random():
            return random.random()
        val1 = get_random()
        val2 = get_random.refresh()
        self.assertNotEquals(val1, val2)

    def test_put(self):
        """Should change the cached value"""
        key = lambda: 'test_put'
        @cached(self.client, ttl=10, key=key)
        def get_number():
            return 1
        val = get_number()
        get_number.put(_cache_put=val+2)
        self.assertEquals(get_number(), val+2)

    def test_peek_not_cached(self):
        """Should raise a CacheMissError"""
        @cached(self.client, ttl=5)
        def not_cached():
            return random.random()
        self.assertRaises(CacheMissError, not_cached.peek)

    def test_peek_cached(self):
        """Shouldn't raise a CacheMissError"""
        @cached(self.client, ttl=5)
        def get_random():
            return random.random()
        val = get_random()
        self.assertEquals(get_random.peek(), val)


class CachedDecoratorFutureTest(DecoratorBaseTest):
    """Tests future functionality for cached decorator"""

    def test_future_invalidate(self):
        """Should delete cache entry"""
        key = lambda: 'test_invalidate'
        @cached(self.client, ttl=10, key=key)
        def get_random():
            return random.random()
        get_random()
        get_random.future().invalidate().result()
        time.sleep(0.1)
        self.assertFalse(get_random.client.contains(key()))

    def test_future_sync_check(self):
        """Should wait and return the value"""
        global val
        @cached(self.client, ttl=5, future_sync_check=True)
        def get_random():
            time.sleep(0.1)
            val = random.random()
            return val
        r = get_random.future()().result()
        self.assertNotEquals(r, val)

    def test_future_no_sync_check(self):
        """Shouldn't wait for the value"""
        @cached(self.client, ttl=5, future_sync_check=False)
        def get_random():
            time.sleep(4)
            return random.random()
        f = get_random.future()()
        self.assertFalse(f.done())

    def test_future_sync_check_on_value(self):
        """Should return the value using on_value function with sync check"""
        global val
        @cached(self.client, ttl=5, async_ttl=10, future_sync_check=True)
        def get_random():
            global val
            val = random.random()
            return val
        get_random()
        r = get_random.future()().result()
        self.assertEquals(r, val)

    def test_future_sync_check_value_loaded(self):
        """Future should return the value instantly"""
        @cached(self.client, ttl=5, future_sync_check=True)
        def get_random():
            return random.random()
        get_random()
        hits = get_random.stats.hits
        get_random.future()().result()
        self.assertEquals(get_random.stats.hits, hits+1)

    def test_future_on_value(self):
        """Should return the value using on_value function"""
        global val
        @cached(self.client, ttl=5)
        def get_random():
            global val
            val = random.random()
            return val
        get_random()
        r = get_random.future()().result()
        self.assertEquals(r, val)
    
    def test_future_lazy(self):
        """Should start calculating the value in background"""
        @cached(self.client, ttl=5)
        def get_number():
            return 8
        rv = get_number.future().lazy()
        self.assertRaises(CacheMissError, rv.result)
        time.sleep(0.1)
        rv = get_number.future().lazy()
        self.assertEquals(rv.result(), 8)

    def test_future_lazy_sync_check_value_loaded(self):
        """Future should return the value instantly"""
        @cached(self.client, ttl=5, async_ttl=10, future_sync_check=True)
        def get_random():
            return random.random()
        get_random()
        hits = get_random.stats.hits
        get_random.future().lazy().result()
        self.assertEquals(get_random.stats.hits, hits+1)

    def test_future_peek_uncached(self):
        """Should raise a CacheMissError"""
        @cached(self.client, ttl=5)
        def get_random():
            return random.random()
        self.assertRaises(CacheMissError, get_random.future().peek().result)

    def test_future_peek_cached_on_value(self):
        """Should call on_value function and return the value"""
        @cached(self.client, ttl=5)
        def get_random():
            return random.random()
        val = get_random()
        self.assertEquals(get_random.future().peek().result(), val)

    def test_future_peek_sync_check(self):
        """Should return the value directly from the client"""
        @cached(self.client, ttl=5, future_sync_check=True)
        def get_random():
            return random.random()
        val = get_random()
        self.assertEquals(get_random.future().peek().result(), val)


class CachedDecoratorAsyncTest(DecoratorBaseTest):
    """Tests async functionality for cached decorator"""

    @classmethod
    def setUpClient(cls):
        super(CachedDecoratorAsyncTest, cls).setUpClient()
        cls.client = AsyncWriteCacheClient(cls.client, 100)

    def test_lazy_async(self):
        """Should raise a CacheMissError and start calculating the value"""
        key = lambda: 'test_lazy_async'
        @cached(self.client, ttl=10, key=key)
        def get_number():
            return 4
        get_number_async = get_number.async()
        self.assertRaises(CacheMissError, get_number_async.lazy)
        time.sleep(0.1)
        self.assertEquals(get_number_async.lazy(), 4)
            
