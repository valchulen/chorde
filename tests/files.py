# -*- coding: utf-8 -*-
import unittest
import shutil
import tempfile
import mmap

from .clientbase import CacheClientTestMixIn, NamespaceWrapperTestMixIn
import chorde.clients.files

class WithTempdir:
    @classmethod
    def _super(cls):
        if WithTempdir in cls.__bases__:
            return cls.__bases__[cls.__bases__.index(WithTempdir)+1]
        else:
            return cls.__bases__[-1]._super()
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self._super().setUp(self)
    def tearDown(self):
        self._super().tearDown(self)
        shutil.rmtree(self.tempdir)

class FilesTest(WithTempdir, CacheClientTestMixIn, unittest.TestCase):
    capacity_means_entries = False
    
    def setUpClient(self):
        from chorde.clients.files import FilesCacheClient
        return FilesCacheClient(1 << 20, self.tempdir, checksum_key = "test")

    def testMmap(self):
        client = self.client
        bigval = "12" * chorde.clients.files.MMAP_THRESHOLD
        client.put("somekey", bigval, 86400)
        cachedval = client.get("somekey")

        self.assertEqual(buffer(bigval), buffer(cachedval))
        self.assertIsInstance(cachedval, mmap.mmap)

class NamespaceFilesTest(NamespaceWrapperTestMixIn, FilesTest):
    def tearDown(self):
        # Manually clear
        self.rclient.clear()

