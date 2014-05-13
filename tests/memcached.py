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

class K:
    pass

@skipIfNoMemcached
class MemcacheTest(CacheClientTestMixIn, unittest.TestCase):
    is_lru = False
    capacity_means_entries = False
    meaningful_capacity = False # controlled externally so it may not be consistent for testing purposes
    
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

    def testStats(self):
        stats = self.client.stats
        self.assertIsNotNone(stats)
        self.assertIsInstance(stats, (dict,list,tuple))

    def testTupleKey(self):
        client = self.client
        client.put((1,2), 2, 10)
        self.assertEqual(client.get((1,2)), 2)

    def testStringKey(self):
        client = self.client
        k = "abracadabra"
        client.put(k, "patadecabra", 10)
        self.assertEqual(client.get(k), "patadecabra")

    def testLongStringKey(self):
        client = self.client
        k = "abracadabra" 
        k = k * (getattr(self.client, 'max_backing_key_length', 2048) / len(k) + 1)
        client.put(k, "patadecabra2", 10)
        self.assertEqual(client.get(k), "patadecabra2")

    def testUTFStringKey(self):
        client = self.client
        k = u"ábracadíbra".encode("utf8")
        client.put(k, "patadecabra", 10)
        self.assertEqual(client.get(k), "patadecabra")

    def testLongUTFStringKey(self):
        client = self.client
        k = u"ábracadíbra".encode("utf8")
        k = k * (getattr(self.client, 'max_backing_key_length', 2048) / len(k) + 1)
        client.put(k, "patadecabra2", 10)
        self.assertEqual(client.get(k), "patadecabra2")

    def testUnicodeStringKey(self):
        client = self.client
        k = u"ábracadíbra"
        client.put(k, "patadecabra", 10)
        self.assertEqual(client.get(k), "patadecabra")

    def testLongUnicodeStringKey(self):
        client = self.client
        k = u"ábracadíbra"
        k = k * (getattr(self.client, 'max_backing_key_length', 2048) / len(k) + 1)
        client.put(k, "patadecabra2", 10)
        self.assertEqual(client.get(k), "patadecabra2")

    def testSpacedStringKey(self):
        client = self.client
        k = "abra cadabra"
        client.put(k, "patadecabra3", 10)
        self.assertEqual(client.get(k), "patadecabra3")

    def testSpacedLongStringKey(self):
        client = self.client
        k = "abra cadabra" 
        k = k * (getattr(self.client, 'max_backing_key_length', 2048) / len(k) + 1)
        client.put(k, "patadecabra4", 10)
        self.assertEqual(client.get(k), "patadecabra4")

    def testObjectKey(self):
        client = self.client
        k = K()
        client.put(k, 15, 10)
        self.assertEqual(client.get(k), 15)

    def testNullKey(self):
        client = self.client
        client.put(None, 15, 10)
        self.assertEqual(client.get(None), 15)

    testClear = unittest.expectedFailure(CacheClientTestMixIn.testClear)
    testPurge = unittest.expectedFailure(CacheClientTestMixIn.testPurge)

@skipIfNoMemcached
class NamespaceMemcacheTest(NamespaceWrapperTestMixIn, MemcacheTest):
    def tearDown(self):
        # Manually clear memcached
        self.rclient.client.flush_all()

    testStats = None

@skipIfNoMemcached
class BuiltinNamespaceMemcacheTest(MemcacheTest):
    def setUpClient(self):
        from chorde.clients.memcached import MemcachedClient
        import threading
        rv = MemcachedClient([DEFAULT_CLIENT_ADDR], 
            checksum_key = "test",
            namespace = "testns",
            encoding_cache = threading.local() )
        rv.client.flush_all()
        return rv

@skipIfNoMemcached
class FastMemcacheTest(CacheClientTestMixIn, unittest.TestCase):
    is_lru = False
    capacity_means_entries = False
    meaningful_capacity = False # controlled externally so it may not be consistent for testing purposes
    
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

    # Not supported by the fast client
    testTupleKey = None
    testLongStringKey = None
    testSpacedStringKey = None
    testSpacedLongStringKey = None
    testObjectKey = None

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

    testStats = None
