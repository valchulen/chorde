# -*- coding: utf-8 -*-
import unittest
import time
import os
from .clientbase import CacheClientTestMixIn, NamespaceWrapperTestMixIn, CacheMissError

DEFAULT_CLIENT_ADDR = os.getenv("MEMCACHE_ADDR", "localhost:11211")

def memcachedBuiltIn():
    try:
        import chorde.clients.memcached  # lint:ok
    except ImportError:
        return False

    try:
        # Test memcached reachability
        import memcache # lint:ok
    except ImportError:
        return False
    
    return True

def memcachedReachable():
    try:
        import memcache
    except ImportError:
        return False
    
    c = memcache.Client([DEFAULT_CLIENT_ADDR])
    return bool(c.get_stats())

if not memcachedBuiltIn():
    skipIfNoMemcached = unittest.skip("Memcached support not built in")
elif not memcachedReachable():
    skipIfNoMemcached = unittest.skip("no memcached reachable at %r" % (DEFAULT_CLIENT_ADDR,))
else:
    skipIfNoMemcached = lambda c : c

@skipIfNoMemcached
class MemcacheTest(CacheClientTestMixIn, unittest.TestCase):
    is_lru = False
    capacity_means_entries = False
    
    def setUpClient(self):
        from chorde.clients.memcached import MemcachedClient
        import threading
        rv = MemcachedClient([DEFAULT_CLIENT_ADDR], 
            checksum_key = "test",
            encoding_cache = threading.local() )
        rv.client.flush_all()
        return rv
    def tearDown(self):
        # Manually clear memcached
        self.client.client.flush_all()

    def testSucceedFast(self):
        client = self.client
        val = "a" * 2
        client.put(4, val, 10)
        self.assertEqual(client.get(4, None), val)
        val = client.get(4, None)
        self.assertIs(client.get(4, None), val)

    testClear = unittest.expectedFailure(CacheClientTestMixIn.testClear)
    testPurge = unittest.expectedFailure(CacheClientTestMixIn.testPurge)

@skipIfNoMemcached
class NamespaceMemcacheTest(NamespaceWrapperTestMixIn, MemcacheTest):
    def tearDown(self):
        # Manually clear memcached
        self.rclient.client.flush_all()

@skipIfNoMemcached
class FastMemcacheTest(CacheClientTestMixIn, unittest.TestCase):
    is_lru = False
    capacity_means_entries = False
    
    def setUpClient(self):
        from chorde.clients.memcached import FastMemcachedClient
        rv = FastMemcachedClient([DEFAULT_CLIENT_ADDR])
        rv.client.flush_all()
        return rv
    def tearDown(self):
        # Manually clear memcached
        self.client.client.flush_all()

    testClear = unittest.expectedFailure(CacheClientTestMixIn.testClear)
    testPurge = unittest.expectedFailure(CacheClientTestMixIn.testPurge)

@skipIfNoMemcached
class FastFailFastMemcacheTest(FastMemcacheTest):
    def setUpClient(self):
        from chorde.clients.memcached import FastMemcachedClient
        self.client2 = FastMemcachedClient([DEFAULT_CLIENT_ADDR], failfast_time = 1, failfast_size = 100)
        rv = FastMemcachedClient([DEFAULT_CLIENT_ADDR], failfast_time = 1, failfast_size = 100)
        rv.client.flush_all()
        return rv

    def tearDown(self):
        # Manually clear memcached
        FastMemcacheTest.tearDown(self)
        self.client2.client.flush_all()

    def testFailFast(self):
        client = self.client
        client2 = self.client2
        
        self.assertRaises(CacheMissError, client.get, 1)
        self.assertRaises(CacheMissError, client2.get, 1)

        # Put, second client still fails fast
        client.put(1, 2, 10)
        self.assertEqual(client.get(1), 2)
        self.assertRaises(CacheMissError, client2.get, 1)

        # Purge second client, clears failfast, so now it sees it
        # Note, must wait a bit for first client to actually write
        time.sleep(0.05)
        client2.purge()
        self.assertEqual(client2.get(1), 2)

@skipIfNoMemcached
class NamespaceFastMemcacheTest(NamespaceWrapperTestMixIn, FastMemcacheTest):
    def tearDown(self):
        # Manually clear memcached
        self.rclient.client.flush_all()
