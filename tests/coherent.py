# -*- coding: utf-8 -*-
import unittest

from chorde.clients import coherent
from chorde.clients import async
from chorde.clients.inproc import InprocCacheClient
from chorde.clients.tiered import TieredInclusiveClient

class CoherentDeferTest(unittest.TestCase):

    def setUp(self):
        self.private = InprocCacheClient(100)
        self.shared = InprocCacheClient(100)
        self.tiered = TieredInclusiveClient(self.private, self.shared)
        self.called = False

    def _call_target(self):
        self.called = True

    def test_stale_reget(self):
        key = 'blarg'
        self.private.put(key, 10, 30)
        self.shared.put(key, 15, 300)
        self.assertTrue(self.shared.contains(key, 60))
        self.assertTrue(self.tiered.contains(key, 60))
        self.assertEqual(self.tiered.get(key), 10)

        defer = coherent.CoherentDefer(self._call_target,
            key = key,
            manager = None,
            expired = lambda : not self.shared.contains(key, 60),
            expire_private = self.private.expire,
            timeout = 1)
        defer.future = async.Future()
        value = defer.undefer()

        self.assertIs(value, async.REGET)
        self.assertEqual(self.tiered.get(key), 15)
