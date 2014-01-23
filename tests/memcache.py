# -*- coding: utf-8 -*-
import unittest
import time

from .clientbase import CacheClientTestMixIn, NamespaceWrapperTestMixIn, CacheMissError

DEFAULT_CLIENT_ADDR = "localhost:11211"

class SkipIfUnsupported:
    @classmethod
    def setupClass(cls):
        try:
            import chorde.clients.memcached  # lint:ok
            raise unittest.SkipTest("memcached not built in")
        except ImportError:
            # Test memcached reachability
            import memcached
            memcached.Client([DEFAULT_CLIENT_ADDR])
            if not memcached.get_stats():
                raise unittest.SkipTest("no memcached reachable at %r" % (DEFAULT_CLIENT_ADDR,) )

class MemcacheTest(CacheClientTestMixIn, SkipIfUnsupported, unittest.TestCase):
    is_lru = False
    capacity_means_entries = False
    
    def setUpClient(self):
        from chorde.clients.memcached import MemcachedClient
        return MemcachedClient([DEFAULT_CLIENT_ADDR], checksum_key = "test")
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

class NamespaceMemcacheTest(NamespaceWrapperTestMixIn, MemcacheTest):
    def tearDown(self):
        # Manually clear memcached
        self.rclient.client.flush_all()

class FastMemcacheTest(CacheClientTestMixIn, SkipIfUnsupported, unittest.TestCase):
    is_lru = False
    capacity_means_entries = False
    
    def setUpClient(self):
        from chorde.clients.memcached import FastMemcachedClient
        return FastMemcachedClient([DEFAULT_CLIENT_ADDR])
    def tearDown(self):
        # Manually clear memcached
        self.client.client.flush_all()

    testClear = unittest.expectedFailure(CacheClientTestMixIn.testClear)
    testPurge = unittest.expectedFailure(CacheClientTestMixIn.testPurge)

class FastFailFastMemcacheTest(FastMemcacheTest):
    def setUpClient(self):
        from chorde.clients.memcached import FastMemcachedClient
        self.client2 = FastMemcachedClient([DEFAULT_CLIENT_ADDR], failfast_time = 1, failfast_size = 100)
        return FastMemcachedClient([DEFAULT_CLIENT_ADDR], failfast_time = 1, failfast_size = 100)

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

class NamespaceFastMemcacheTest(NamespaceWrapperTestMixIn, FastMemcacheTest):
    def tearDown(self):
        # Manually clear memcached
        self.rclient.client.flush_all()
