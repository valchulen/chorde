# -*- coding: utf-8 -*-
import unittest
import shutil
import tempfile
import time
import mmap

import os.path

from .clientbase import CacheClientTestMixIn, NamespaceWrapperTestMixIn

SIZE = 1 << 20

# Try to guess OS-dependent mtime resolution
if os.name == 'posix':
    TIME_RESOLUTION = 0.01
elif os.name == 'nt':
    TIME_RESOLUTION = 1
else:
    TIME_RESOLUTION = 0.5

try:
    import chorde.clients.files
    skipIfNoFiles = lambda c : c
except:
    skipIfNoFiles = unittest.skip("Files support not built in (missing dependencies for shmemutils)")

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

@skipIfNoFiles
class FilesTest(WithTempdir, CacheClientTestMixIn, unittest.TestCase):
    capacity_means_entries = False

    def setUpClient(self):
        from chorde.clients.files import FilesCacheClient
        return FilesCacheClient(SIZE, self.tempdir,
            checksum_key = "test",
            sync_purge = 1,
            mmap_raw = True)

    def testMmap(self):
        client = self.client
        bigval = "12" * chorde.clients.files.MMAP_THRESHOLD
        client.put("somekey", bigval, 86400)
        cachedval = client.get("somekey")

        self.assertEqual(buffer(bigval), buffer(cachedval))
        self.assertIsInstance(cachedval, mmap.mmap)

    def testFile(self):
        client = self.client

        tmp = tempfile.NamedTemporaryFile(dir=self.tempdir)
        self.assertTrue(os.path.exists(tmp.name))
        self.assertFalse(client.contains("weefile"))

        rnd = os.urandom(1024)
        tmp.write(rnd)
        tmp.flush()
        client.put("weefile", tmp, 86400)
        self.assertTrue(os.path.exists(tmp.name)) # not deleted
        self.assertTrue(client.contains("weefile"))

        tmp.close()
        self.assertFalse(os.path.exists(tmp.name))
        del tmp

        tmp = client.get("weefile")
        self.assertIsInstance(tmp, file)
        self.assertTrue(os.path.exists(tmp.name))
        self.assertEqual(tmp.read(), rnd)
        tmp.close()
        self.assertTrue(os.path.exists(tmp.name))

        tmp = tempfile.NamedTemporaryFile(dir=self.tempdir)
        self.assertTrue(os.path.exists(tmp.name))
        self.assertTrue(client.contains("weefile"))

        rnd = os.urandom(1024)
        tmp.write(rnd)
        tmp.flush()
        client.put("weefile", tmp, 86400)
        self.assertTrue(os.path.exists(tmp.name)) # not deleted
        self.assertTrue(client.contains("weefile"))

        tmp.close()
        self.assertFalse(os.path.exists(tmp.name))
        del tmp

        tmp = client.get("weefile")
        self.assertIsInstance(tmp, file)
        self.assertTrue(os.path.exists(tmp.name))
        self.assertEqual(tmp.read(), rnd)
        tmp.close()
        self.assertTrue(os.path.exists(tmp.name))

    def testLRU(self):
        client = self.client
        bigval = "12" * chorde.clients.files.MMAP_THRESHOLD
        maxentries = SIZE / len(bigval)

        for i in xrange(maxentries+1):
            client.put(i, bigval+str(i), 86400)

            time.sleep(TIME_RESOLUTION*2)

            if i > 0:
                self.assertTrue(client.contains(i-1))
            self.assertTrue(client.contains(i))
            cachedval = client.get(i)
            self.assertTrue(buffer(cachedval, 0, len(bigval)) == buffer(bigval))
            self.assertTrue(buffer(cachedval, len(bigval)) == buffer(str(i)))
            del cachedval
        self.assertFalse(client.contains(0))
        self.assertTrue(client.contains(maxentries/2))

    def testLimit(self):
        # Gotta be lenient on usage tests, since there's unknown overhead
        client = self.client
        cap = client.capacity
        bigval = "12" * chorde.clients.files.MMAP_THRESHOLD
        maxentries = SIZE / len(bigval)

        for i in xrange(maxentries*2):
            client.put(i,bigval,86400)
            self.assertTrue(client.usage >= (len(bigval) * min(i, maxentries-1)))
            self.assertLess(client.usage, cap + len(bigval))

    def testCounterReset(self):
        client = self.client
        client.put(1, 1, 86400)
        usage = client.usage
        client.close()

        # Break it
        with open(os.path.join(client.basepath, "sizemap.32.100"), "w") as f:
            f.seek(0, os.SEEK_END)
            sz = f.tell()
            f.seek(0)
            f.write("\xff" * sz)

        self.client = client = self.setUpClient()
        self.assertEqual(client.usage, usage)

class NamespaceFilesTest(NamespaceWrapperTestMixIn, FilesTest):
    def tearDown(self):
        # Manually clear
        self.rclient.clear()

    def testCounterReset(self):
        pass
