# -*- coding: utf-8 -*-
import unittest

from .clientbase import CacheClientTestMixIn, NamespaceWrapperTestMixIn

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

class NamespaceFastMemcacheTest(NamespaceWrapperTestMixIn, FastMemcacheTest):
    def tearDown(self):
        # Manually clear memcached
        self.rclient.client.flush_all()
