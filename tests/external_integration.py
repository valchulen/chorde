# -*- coding: utf-8 -*-

from unittest import TestCase, skipIf

from chorde.external_integration import monkey_patch_tornado
from chorde.clients.async import Future

try:
    import tornado
    from tornado.testing import AsyncTestCase, gen_test
    from tornado.gen import coroutine, Return, BadYieldError
    from tornado import concurrent
    imported_tornado = True
except:
    AsyncTestCase = TestCase
    imported_tornado = False
    gen_test = lambda x: x


@skipIf(not imported_tornado, 'Tornado not found. Skipping')
class TestTornadoIntegration(AsyncTestCase):

    def setUp(self):
        super(TestTornadoIntegration, self).setUp()
        original_futures = concurrent.FUTURES

        @coroutine
        def foo():
            f = Future()
            f.set(1)
            rv = yield f
            raise Return(rv)

        def restore_isfuture():
            concurrent.FUTURES = original_futures

        self.addCleanup(restore_isfuture)
        self.foo = foo

    @gen_test
    def test_no_patch(self):
        with self.assertRaises(BadYieldError):
            yield self.foo()

    @gen_test
    def test_patched(self):
        monkey_patch_tornado()
        rv = yield self.foo()
        self.assertEqual(1, rv)

    @gen_test
    def test_patched_more_than_once(self):
        monkey_patch_tornado()
        monkey_patch_tornado()
        rv = yield self.foo()
        self.assertEqual(1, rv)

        self.assertEqual(1, concurrent.FUTURES.count(Future))

