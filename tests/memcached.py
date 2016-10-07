# -*- coding: utf-8 -*-
import unittest
import time
import os
from .clientbase import CacheClientTestMixIn, NamespaceWrapperTestMixIn, CacheMissError
from .base import TestCase

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

try:
    import lz4  # lint:ok
    skipIfNoLZ4 = lambda c : c
except ImportError:
    skipIfNoLZ4 = unittest.skip("lz4 support not built in")

class K:
    pass

@skipIfNoMemcached
class MemcacheTest(CacheClientTestMixIn, TestCase):
    is_lru = False
    capacity_means_entries = False
    meaningful_capacity = False # controlled externally so it may not be consistent for testing purposes

    # Big uncompressible (but ascii-compatible) value
    BIG_VALUE = os.urandom(4 << 20).encode("base64")

    def setUpClient(self, **kwargs):
        from chorde.clients.memcached import MemcachedClient
        import threading
        rv = MemcachedClient([DEFAULT_CLIENT_ADDR], 
            checksum_key = "test",
            encoding_cache = threading.local(),
            **kwargs )
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

    def testBigValue(self):
        bigval = self.BIG_VALUE
        client = self.client
        client.put("bigkey1", bigval, 60)
        self.assertEqual(client.get("bigkey1"), bigval)

    def testRenewBigValue(self):
        bigval = self.BIG_VALUE
        client = self.client
        client.put("bigkey2", bigval, 10)
        client.renew("bigkey2", 120)
        self.assertGreater(client.getTtl("bigkey2")[1], 10)

    def testContainsBigValue(self):
        bigval = self.BIG_VALUE
        client = self.client
        client.put("bigkey3", bigval, 10)
        self.assertTrue(client.contains("bigkey3"))

    def testContainsBigValueTTLExact(self):
        bigval = self.BIG_VALUE
        client = self.client
        client.put("bigkey3", bigval, 10)
        self.assertTrue(client.contains("bigkey3"))

    def testContainsBigValueTTLInexact(self):
        bigval = self.BIG_VALUE
        client = self.client
        bigkey = "bigkey3" * 500
        client.put(bigkey, bigval, 10)

        # Force re-decoding
        if hasattr(client, 'encoding_cache'):
            client.encoding_cache.cache = None
        
        self.assertTrue(client.contains(bigkey, 1))

    def testTTLTooBig(self):
        from chorde.clients.memcached import MAX_MEMCACHE_TTL 
        client = self.client
        k = "abra cadabra"
        client.put(k, "patadecabra3", MAX_MEMCACHE_TTL * 2)
        self.assertEqual(client.get(k), "patadecabra3")
        self.assertTrue(MAX_MEMCACHE_TTL * 2 - 1 <= client.getTtl(k)[1] <= MAX_MEMCACHE_TTL * 2)



    testClear = unittest.expectedFailure(CacheClientTestMixIn.testClear)
    testPurge = unittest.expectedFailure(CacheClientTestMixIn.testPurge)

@skipIfNoMemcached
class ElastiCacheTest(MemcacheTest):
    def setUpClient(self, **kwargs):
        from chorde.clients.elasticache import ElastiCacheClient
        import threading
        rv = ElastiCacheClient([DEFAULT_CLIENT_ADDR],
            checksum_key = "test",
            encoding_cache = threading.local(),
            **kwargs)
        rv.client.flush_all()
        return rv

@skipIfNoMemcached
class NamespaceMemcacheTest(NamespaceWrapperTestMixIn, MemcacheTest):
    def tearDown(self):
        # Manually clear memcached
        self.rclient.client.flush_all()

    testStats = unittest.skip("not applicable")(MemcacheTest.testStats)

@skipIfNoMemcached
class UncompressedMemcacheTest(MemcacheTest):
    def setUpClient(self):
        return super(UncompressedMemcacheTest, self).setUpClient(
            compress = False)

@skipIfNoMemcached
class CustomPicklerMemcacheTest(MemcacheTest):
    def setUpClient(self):
        import json
        return super(CustomPicklerMemcacheTest, self).setUpClient(
            pickler = json)

    def testObjectKey(self):
        # This should fail
        client = self.client
        k = K()
        self.assertRaises(TypeError, client.put, k, 15, 10)

@skipIfNoMemcached
class CustomClientPicklerMemcacheTest(MemcacheTest):
    def setUpClient(self):
        try:
            import cPickle
        except ImportError:
            import pickle as cPickle  # lint:ok
        return super(CustomClientPicklerMemcacheTest, self).setUpClient(
            client_pickler = cPickle.Pickler,
            client_unpickler = cPickle.Unpickler,
            compress = False)

@skipIfNoMemcached
@skipIfNoLZ4
class LZ4MemcacheTest(MemcacheTest):
    def setUpClient(self):
        import chorde.clients.memcached
        return super(LZ4MemcacheTest, self).setUpClient(
            compress_prefix = chorde.clients.memcached.lz4_compress_prefix,
            compress_file_class = chorde.clients.memcached.lz4_compress_file_class,
            decompress_fn = chorde.clients.memcached.lz4_decompress_fn)

@skipIfNoMemcached
class BuiltinNamespaceMemcacheTest(NamespaceWrapperTestMixIn, MemcacheTest):
    def _setUpClient(self):
        from chorde.clients.memcached import MemcachedClient
        import threading
        self.rclient = self.setUpClient()
        self.rclient.client.flush_all()
        self.bclient = MemcachedClient([DEFAULT_CLIENT_ADDR], 
            checksum_key = "test2",
            namespace = "testns1",
            encoding_cache = threading.local() )
        return MemcachedClient([DEFAULT_CLIENT_ADDR], 
            checksum_key = "test3",
            namespace = "testns2",
            encoding_cache = threading.local() )

    # We don't implement clear
    testNamespaceClear = unittest.skip("not applicable")(NamespaceWrapperTestMixIn.testNamespaceClear)

@skipIfNoMemcached
class FastMemcacheTest(CacheClientTestMixIn, TestCase):
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
    testBigValue = None
    testRenewBigValue = None
    testContainsBigValue = None
    testContainsBigValueTTLExact = None
    testContainsBigValueTTLInexact = None

@skipIfNoMemcached
class FastElastiCacheTest(FastMemcacheTest):
    def setUpClient(self):
        from chorde.clients.elasticache import FastElastiCacheClient
        rv = FastElastiCacheClient([DEFAULT_CLIENT_ADDR])
        rv.client.flush_all()
        return rv

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
