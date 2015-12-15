# -*- coding: utf-8 -*-
from . import base, inproc
from .base import NONE, CacheMissError
from chorde import shmemutils, sPickle, serialize

import functools
import hashlib
import heapq
import json
import logging
import mmap
import os.path
import shutil
import tempfile
import thread
import threading
import time
import weakref

_caches_mutex = threading.RLock()
_caches = weakref.WeakKeyDictionary()

MMAP_THRESHOLD = 1 << 14

def _register_files(cache):
    _caches[cache] = None

def cachePurge(timeout = 0):
    with _caches_mutex:
        caches = _caches.keys()
    
    for cache in caches:
        cache.purge(timeout)

def cacheClear():
    with _caches_mutex:
        caches = _caches.keys()
    
    for cache in caches:
        cache.clear()

class CacheJanitorThread(threading.Thread):

    def __init__(self, sleep_interval, purge_timeout = 0):
        threading.Thread.__init__(self)
        self.sleep_interval = sleep_interval
        self.purge_timeout = purge_timeout
        self.logger = None
        self.setDaemon(True)
        
    def run(self):
        global cachePurge
        
        while True:
            time.sleep(self.sleep_interval)
            try:
                cachePurge(self.purge_timeout)
            except:
                if self.logger is None:
                    pass
                else:
                    self.logger.error("Exception during cache purge", exc_info = True)

def startCacheJanitorThread(sleep_interval=3600, purge_timeout=0):
    jthread = CacheJanitorThread(sleep_interval, purge_timeout)
    jthread.start()
    return jthread

def _tmpsuffix():
    return ".tmp.%d.%x.%x" % (os.getpid(),thread.get_ident(),int(time.time()))

def _touch(path):
    try:
        os.utime(path, (time.time(), os.path.getmtime(path)))
    except OSError:
        pass

def _swap(source, dest, sizeback = None):
    cursize = 0
    if sizeback is not None:
        # remember the existing file's size
        try:
            cursize = os.path.getsize(dest)
        except:
            pass
    try:
        os.rename(source, dest)
    except OSError:
        # Do an indirect swap, in case it was on a different filesystem
        tmpname = dest+_tmpsuffix()
        try:
            # This might work still, on systems with os.link where rename doesn't overwrite
            os.link(source, tmpname)
        except:
            shutil.copy2(source, tmpname)
        try:
            # Try again the rename, if the above did the copy, it might work now
            os.rename(tmpname, dest)
        except:
            try:
                # Non-atomic replace, needed in windows
                os.unlink(dest)
                os.rename(tmpname, dest)
            except:
                _clean(tmpname)
                raise
        os.unlink(source)

    if sizeback is not None:
        # Call with size delta
        sizeback(os.path.getsize(dest) - cursize)

def _link(source, dest, sizeback = None, filemode = None):
    cursize = 0
    if sizeback is not None:
        # remember the existing file's size
        try:
            cursize = os.path.getsize(dest)
        except:
            pass
    try:
        os.link(source, dest)
    except OSError:
        # Do an indirect swap, in case it was on a different filesystem
        tmpname = dest+_tmpsuffix()
        shutil.copy2(source, tmpname)
        if filemode is not None:
            os.chmod(tmpname, filemode)
        try:
            # Try again the rename, it might work now
            os.rename(tmpname, dest)
        except:
            try:
                # Non-atomic replace, needed in windows
                os.unlink(dest)
                os.rename(tmpname, dest)
            except:
                _clean(tmpname)
                raise

    if sizeback is not None:
        # Call with size delta
        sizeback(os.path.getsize(dest) - cursize)

def _clean(path, sizeback = None):
    if sizeback is not None:
        # remember size, for the callback
        try:
            sizeback = functools.partial(sizeback, os.path.getsize(path))
        except:
            # Doesn't exist...
            return
    
    try:
        os.unlink(path)
    except:
        pass

    if sizeback is not None:
        sizeback()

class FilesCacheClient(base.BaseCacheClient):
    """
    This cache client will store itmes on a file hierarchy.
    Depending on the operating system, the files need not be backed by
    persistent storage (ie: it could be temporary in-memory storage, making
    it quite fast), but persistence is a possibility, and a common reason to use
    this client.

    This client will behave specially when given file objects. Normal clients can't
    or shouldn't store file objects, but this client has been specifically designed
    for that task. File objects thus cached must contain a 'name' attribute that
    points to an absolute path to the file.

    When a file object is added to the client, it is copied in the most efficient
    manner into the cache's file heirarchy. On posix systems, this will create
    a hardlink when the file is on the same filesystem as the cache's store,
    and it is thus the recommended use case.

    When byte strings are given, they will be written to a file within the cache's
    file heirarchy. Retrieving them is transparent, unless mmap_raw is given and True.
    In that case, queries to the cache will return a buffer (maybe a string, maybe
    an mmap object, depending on the value's size), and thus the cache will not be
    transparent, but it will be more efficient as it will share memory across processes.

    This cache is limited by size in bytes. The size includes book-keeping overhead,
    so be certain to account for serialized key sizes when specifying it.

    The client is thread and process-safe, but it needs a purging thread to be
    running, either manually calling purge() every once in a while, or by calling
    startCacheJanitorThread() with appropriate arguments.

    If entries are added too fast for the purging thread, that is, if considerably
    more bytes than the limit are added between purging runs, it is possible for this
    cache to grow past the size limit. If this is a concern, specify sync_purge as
    the highest acceptable multiple of the cache's size limit. When reached, a
    synchronous purge will be attempted before returning, guaranteeing adherence
    to given size limits.
    """

    def __init__(self, size, basepath,
            failfast_size = 500, failfast_time = 0.25, counter_slots = 256,
            key_pickler = json.dumps, value_pickler = None, value_unpickler = None, value_opener=None, checksum_key = None,
            dirmode = 0700, filemode = 0400, mmap_raw = False,
            sync_purge = None):
        """
        Params:
            size: Maximum cache size, in bytes, counting key serialization and book-keeping overheads

            failfast_size: Entries in a small key-only inproc cache used to speed up cache misses

            failfast_time: TTL of the failfast cache

            counter_slots: Number of PID slots in the cached size counter file. In order to make size
                updates wait-free, each process uses a separate entry. In platforms where fcntl.lock
                is implemented, this needs only be as big as the number of processes that will attempt
                to concurrently update this cache, plus some free space to allow dying processes to
                respawn (their slot will be unusable). In other platforms, this should be 65536, since
                otherwise hash collisions would prevent the cache from starting (there's no atomic
                way of grabbing a counter slot implemented in those platforms). The default should
                work on most posix platforms.

            key_pickler: Pickling *function* used to serialize keys. By default, it's json. This 
                function should be stable: equal keys must serialize to equal strings. 
                Important Note: Pickle doesn't always respect this invariant.

            value_pickler: Pickling *function* used to serialize values. It should take a signature
                compatible with pickle.dump, including protocol version. By default, it's built
                out of sPickle.dump and the checksum_key (which is then mandatory)

            value_unpickler: Unpickling *function* used to de-serialize values. It should take a
                signature compatible with pickle.load. By default, it's built out of sPickle.load
                and the checksum_key (which is then mandatory). Always validate contents if this
                function is based on pickling, otherwise malicious injection into the cache
                file heirarchy could result in arbitrary code execution.

            value_opener: opener *function* used to return non-file object. It should take a fileobject parameter

            checksum_key: When using the default picklers, this *private* key is necessary
                in order to authenticate values and make sure they have been written by this process
                (or any process with access to the key). Otherwise, malicious injection into the
                cache's file heirarchy could result in arbitrary code execution, and file corruption
                even if not malicious, could result in hard crashes (out-of-memory, segmentation fault, etc).

            dirmode: Permissions bitfield used for creating directories. See os.chmod

            filemode: Permissions bitfield used for cache files. See os.chmod

            mmap_raw: Whether to return mmap objects when reading large byte strings from the cache,
                instead of string copies of the contents. This is far more efficient than reading the
                whole contents of the file, but the returned buffer isn't fully compatible with string semantics,
                so by default it's False.

            sync_purge: A (possibly float) multiplier of size that will trigger a synchronous purge when
                reached. To avoid the long pauses that could be generated by those forced purge cycles, this
                is None (no sync purges) by default.
        """
        self._failfast_cache = inproc.Cache(failfast_size)
        self.failfast_time = failfast_time
        self.basepath = basepath
        self.max_size = size
        self.counter_slots = counter_slots
        self.size = self._make_counter()
        self.key_pickler = key_pickler
        self.checksum_key = checksum_key
        self.filemode = filemode
        self.dirmode = dirmode
        self.mmap_raw = mmap_raw
        self.sync_purge = sync_purge

        if value_opener is None:
            value_opener = open
        self.value_opener = value_opener

        if value_pickler is None or value_unpickler is None:
            if checksum_key is None:
                raise ValueError, "Must be given a checksum_key when using the default pickler"
            else:
                self.value_pickler = functools.partial(sPickle.dump, checksum_key)
                self.value_unpickler = functools.partial(sPickle.load, checksum_key)
        elif value_pickler is not None or value_unpickler is not None:
            raise ValueError, "Must be given both pickler and unpickler when using custom picklers"
        else:
            self.value_pickler = value_pickler
            self.value_unpickler = value_unpickler
        
        _register_files(self)

    def _make_counter(self):
        counter_slots = self.counter_slots
        if self.max_size > (1<<30):
            cls = shmemutils.SharedCounter64
            basename = "sizemap.64"
        else:
            cls = shmemutils.SharedCounter32
            basename = "sizemap.32"
        path = os.path.join(self.basepath, "%s.%x" % (basename, counter_slots))

        try:
            rv = cls.from_path(counter_slots, path)
        except:
            # ouch, broken counter, reset

            # Create a new one
            tpath = path + _tmpsuffix()
            rv = cls.from_path(counter_slots, tpath)
            rv.close()

            # Swap it
            _swap(tpath, path)

            # Re-open it
            rv = cls.from_path(counter_slots, path)
        if int(rv) == 0:
            logger = logging.getLogger('chorde')

            t0 = time.time()
            logger.info("Initializing concurrent counter for file-based cache on %r", self.basepath)
            
            # Um... uninitialized
            # Compute size
            actual_size = self._compute_size()

            # Computing takes time, check to see whether some other guy beat us to it
            if int(rv) == 0:
                # Nop, set
                rv += actual_size
                rv.flush()
            
            t1 = time.time()
            logger.info("Initialized concurrent counter for file-based cache on %r (%.3fs)", 
                self.basepath, t1 - t0)

        return rv

    def _walk(self):
        for dirpath, dirs, files in os.walk(self.basepath):
            if dirpath != self.basepath:
                for fname in files:
                    yield os.path.join(dirpath, fname)

    def _compute_size(self):
        rv = 0
        getsize = os.path.getsize
        join = os.path.join
        for fpath in self._walk():
            try:
                rv += getsize(join(self.basepath, fpath))
            except OSError:
                pass # Meh, entries are transient
        return rv

    def _mkpath(self, key):
        khash = hashlib.md5(key).hexdigest()
        return (khash[:2], khash[2:4], khash[4:6], khash[6:])

    def _mktmp(self):
        tmpdir = os.path.join(self.basepath, "staging")
        if not os.path.exists(tmpdir):
            try:
                os.makedirs(tmpdir, self.dirmode)
            except:
                if not os.path.exists(tmpdir):
                    raise
        return tempfile.NamedTemporaryFile(dir=tmpdir)

    @property
    def async(self):
        return False

    @property
    def capacity(self):
        return self.max_size

    @property
    def usage(self):
        return int(self.size)

    def _put(self, key, value, ttl, replace):
        key = self.key_pickler(key)
        kpath = self._mkpath(key)

        targetpath = os.path.join(self.basepath, *kpath)
        keypath = targetpath + ".key"
        targetdir = os.path.join(self.basepath, *kpath[:-1])

        if not os.path.exists(targetdir):
            try:
                os.makedirs(targetdir, self.dirmode)
            except OSError:
                if not os.path.exists(targetdir):
                    raise

        reuse_keyfile = False
        if os.path.exists(keypath):
            # Huh... compare
            if os.path.getsize(keypath) == len(key):
                with open(keypath) as ekey:
                    if ekey.read() == key:
                        reuse_keyfile = True
            if reuse_keyfile and not replace:
                # Um... check the validity of the current value before going further
                if os.path.getmtime(keypath) >= time.time():
                    # Still valid, don't replace
                    return False

        def setTtl(path):
            now = time.time()
            os.utime(path, (now, now + ttl))

        def setPermsf(fileobj):
            os.fchmod(fileobj.fileno(), self.filemode)
        
        if not reuse_keyfile:
            # Create a key file
            # Yeah, can't use a context manager, pity
            keyfile = self._mktmp()
            try:
                keyfile.write(key)
                keyfile.flush()
            except:
                keyfile.close()
                raise
            
            # On posix systems, where swap does an atomic rename, 
            # which preserves source attributes, this avoids instants
            # where the entry is there but expired. Its TTL will be reset
            # with the proper time from insertion in the cache later on.
            setTtl(keyfile.name)
            setPermsf(keyfile)
        
        try:
            try:
                if hasattr(value, 'fileno') and hasattr(value, 'name') and os.path.isabs(value.name):
                    # Looks like a file, so we can simply link, try
                    name = value.name
                    
                    # Overwiting from unknown location, must take extra care
                    _link(name, targetpath+'.file', self.size.__iadd__, self.filemode)
                    if not reuse_keyfile:
                        _swap(keyfile.name, keypath, self.size.__iadd__)
                        keyfile.delete = False
                    
                    # Succeeded, clean up other representations, if they exist, set ttl
                    _clean(targetpath+'.raw', self.size.__isub__)
                    _clean(targetpath+'.ser', self.size.__isub__)
                    setTtl(keypath)
                    
                    return True
            except:
                logging.error("Oops", exc_info = True)
                # Meh, linking didn't work
                pass
                
            # Else, must stuff the values into a file
            if isinstance(value, (bytes,buffer)):
                # Quite easily... stuff it
                with self._mktmp() as rawfile:
                    rawfile.write(value)
                    rawfile.flush()

                    _swap(rawfile.name, targetpath+'.raw', self.size.__iadd__)
                    rawfile.delete = False
                    if not reuse_keyfile:
                        _swap(keyfile.name, keypath, self.size.__iadd__)
                        keyfile.delete = False
                    
                    # Succeeded, clean up other representations, if they exist
                    _clean(targetpath+'.file', self.size.__isub__)
                    _clean(targetpath+'.ser', self.size.__isub__)
                    setTtl(keypath)
                    
                return True
            else:
                if not self.checksum_key:
                    raise RuntimeError, "Cannot encode arbitrary objects without a checksum key"
                
                with self._mktmp() as rawfile:
                    self.value_pickler(value, rawfile, 2)
                    rawfile.flush()
                    
                    _swap(rawfile.name, targetpath+'.ser', self.size.__iadd__)
                    rawfile.delete = False
                    if not reuse_keyfile:
                        _swap(keyfile.name, keypath, self.size.__iadd__)
                        keyfile.delete = False
                    
                    # Succeeded, clean up other representations, if they exist
                    _clean(targetpath+'.file', self.size.__isub__)
                    _clean(targetpath+'.raw', self.size.__isub__)
                    setTtl(keypath)
                    
                return True
        finally:
            if not reuse_keyfile:
                keyfile.close()

        return True

    def put(self, key, value, ttl):
        self._put(key, value, ttl, True)
        
        try:
            del self._failfast_cache[key]
        except:
            pass

        if self.sync_purge is not None and int(self.size) > (self.max_size * self.sync_purge):
            # Ehm... going overboard man
            try:
                self._do_purge(0)
            except serialize.DeadlockError:
                # Someone's purging already
                pass

    def renew(self, key, ttl):
        key = self.key_pickler(key)
        kpath = self._mkpath(key)

        targetpath = os.path.join(self.basepath, *kpath)
        keypath = targetpath + ".key"
        targetdir = os.path.join(self.basepath, *kpath[:-1])

        if not os.path.exists(targetdir):
            return

        if os.path.exists(keypath):
            # Huh... compare
            if os.path.getsize(keypath) == len(key):
                with open(keypath) as ekey:
                    if ekey.read() == key:
                        # Touch
                        now = time.time()
                        kttl = os.path.getmtime(keypath)
                        nttl = ttl + now
                        if kttl < nttl:
                            os.utime(keypath, (now, now + ttl))

    def add(self, key, value, ttl):
        rv = self._put(key, value, ttl, False)
        
        try:
            del self._failfast_cache[key]
        except:
            pass

        if rv and self.sync_purge is not None and int(self.size) > (self.max_size * self.sync_purge):
            # Ehm... going overboard man
            try:
                self._do_purge(0)
            except serialize.DeadlockError:
                # Someone's purging already
                pass

        return rv

    def delete(self, key):
        key = self.key_pickler(key)
        kpath = self._mkpath(key)
        targetpath = os.path.join(self.basepath, *kpath)

        for suffix in ('.file','.raw','.ser','.key'):
            _clean(targetpath+suffix, self.size.__isub__)

    def expire(self, key):
        key = self.key_pickler(key)
        kpath = self._mkpath(key)
        targetpath = os.path.join(self.basepath, *kpath)

        # Touch all files, invalidating them immediately
        for suffix in ('.file','.raw','.ser','.key'):
            try:
                now = time.time()
                os.utime(targetpath+suffix, (now,now))
            except:
                pass

    def _getTtl(self, key, default = base.NONE, baseNONE = base.NONE, ttl_skip=None, 
            promote_callback = None, decode=True):
        key = self.key_pickler(key)
        kpath = self._mkpath(key)
        targetpath = os.path.join(self.basepath, *kpath)
        keypath = targetpath + '.key'

        if not os.path.exists(keypath):
            return default, -1

        try:
            if os.path.getsize(keypath) != len(key):
                return default, -1
        except OSError:
            return default, -1

        # Check fail-fast cache before opening files and all taht
        if self._failfast_cache.get(key) > (time.time() - self.failfast_time):
            return default, -1
        
        with open(keypath) as ekey:
            if ekey.read() == key:
                # Um... check the validity of the current value before going further
                ttl = os.path.getmtime(keypath)
                rttl = ttl - time.time()

                if ttl_skip is not None and rttl < ttl_skip:
                    return default, -1
                elif decode:
                    try:
                        _touch(keypath) # force atime update, in case it's a non-strict-atime mount
                        if os.access(targetpath+'.file', os.R_OK):
                            return self.value_opener(targetpath+'.file', 'rb') ,rttl
                        elif os.access(targetpath+'.raw', os.R_OK):
                            with open(targetpath+'.raw', 'rb') as rawfile:
                                rawfile.seek(0, os.SEEK_END)
                                size = rawfile.tell()
                                rawfile.seek(0)
                                if size > MMAP_THRESHOLD:
                                    return mmap.mmap(rawfile.fileno(), size, access = mmap.ACCESS_READ), rttl
                                elif size == 0:
                                    return "", rttl
                                else:
                                    return rawfile.read(), rttl
                        elif self.checksum_key and os.access(targetpath+'.ser', os.R_OK):
                            with open(targetpath+'.ser', 'rb') as rawfile:
                                return self.value_unpickler(rawfile), rttl
                        else:
                            # Broken?
                            return default, -1
                    except:
                        # Oops
                        logging.getLogger('chorde').error("Error retrieving file contents", exc_info = True)
                        return default, -1
                else:
                    return default, rttl
            else:
                self._failfast_cache[key] = time.time()
                return default, -1

    def getTtl(self, key, default=NONE, ttl_skip = None, **kw):
        # This trampoline is necessary to avoid re-entrancy issues when this client
        # is wrapped inside a SyncWrapper. Internal calls go directly to _getTtl
        # to avoid locking the wrapper's mutex.
        return self._getTtl(key, default, ttl_skip = ttl_skip, **kw)

    def get(self, key, default=NONE, **kw):
        rv, ttl = self._getTtl(key, default, ttl_skip = 0, **kw)
        if ttl < 0 and default is NONE:
            raise CacheMissError, key
        else:
            return rv
    
    def contains(self, key, ttl = None):
        rv, ettl = self._getTtl(key, ttl_skip = 0, decode = False)
        if ettl < 0:
            return False
        else:
            # Key check was successful, so... forget about past mistakes.
            try:
                del self._failfast_cache[key]
            except:
                pass
            return ettl > ttl

    def close(self):
        # Free up stuff
        self.size.close()
        self.size = None
    
    def clear(self):
        # Bye bye everything
        self.size.flush()
        for dirpath, dirnames, filenames in os.walk(self.basepath):
            for subpath in dirnames:
                try:
                    shutil.rmtree(os.path.join(dirpath, subpath))
                except OSError:
                    logging.error("Oops", exc_info = True)
                    pass
            for subpath in filenames:
                try:
                    os.unlink(os.path.join(dirpath, subpath))
                except OSError:
                    logging.error("Oops", exc_info = True)
                    pass
            del dirnames[:]
            del filenames[:]
        
        # Must reset counter now
        self.size.close()
        self.size = self._make_counter()
        self._failfast_cache.clear()

    def purge(self, timeout = 0):
        # Trampoline necessary to avoid deadlocks if this is wrapped in a SyncWrapper
        self._do_purge(timeout)

    @serialize.serialize
    def _do_purge(self, timeout):
        # Blocking trampoline
        self.__do_purge(timeout)

    @serialize.serialize(deadlock_timeout=0.01)
    def _try_purge(self, timeout):
        # Non-blocking trampoline
        self.__do_purge(timeout)
    
    def __do_purge(self, timeout):
        # Abbreviations to make it more readable
        exists = os.path.exists
        join = os.path.join
        getsize = os.path.getsize
        basepath = self.basepath

        # Since we're at it, compute size
        fullsize = 0
        expired_items = expired_bytes = evicted_items = evicted_bytes = 0

        logger = logging.getLogger('chorde')
        logger.info('Purging file-based cache at %r, cached size %d / %d',
            basepath, int(self.size), self.max_size)

        if int(self.size) > self.max_size:
            # Dingbats... must remove some entries
            deletions = []
            excess = int(self.size) - self.max_size
        else:
            # Don't even bother gathering deletions
            deletions = None
            excess = 0

        expirations = []

        for fpath in self._walk():
            try:
                fullsize += getsize(join(basepath, fpath))
            except OSError:
                pass
            if fpath.endswith('.key'):
                bpath = fpath[:-4]
                fpath = join(basepath, fpath)
                bpath = join(basepath, bpath)
                try:
                    stats = os.stat(fpath)
                except OSError:
                    continue
                if (stats.st_mtime + timeout) < time.time():
                    # Stale key, say bye
                    expirations.append(bpath)
                elif deletions is not None:
                    itemsize = stats.st_size
                    for suffix in ('.key', '.file','.raw','.ser'):
                        try:
                            itemsize += os.path.getsize(bpath+suffix)
                        except OSError:
                            pass
                    delitem = (stats.st_atime, bpath, itemsize)
                    if deletions and excess <= 0:
                        if deletions[0] > delitem:
                            excess -= itemsize
                            xdelitem = heapq.heapreplace(deletions, delitem)
                            excess += xdelitem[2]
                    else:
                        excess -= itemsize
                        heapq.heappush(deletions, delitem)
            else:
                for suffix in ('.file','.raw','.ser'):
                    if fpath.endswith(suffix):
                        bpath = fpath[:-len(suffix)]
                        break
                else:
                    bpath = None
                if bpath is not None:
                    fpath = join(basepath, fpath)
                    bpath = join(basepath, bpath)
                    if not exists(bpath+'.key'):
                        # Deleted entry
                        delta = []
                        _clean(fpath, delta.append)
                        if delta:
                            delta = sum(delta)
                            self.size -= delta
                            fullsize -= delta
                            excess -= delta
                            logger.debug('Removed stray file %r of size %d', fpath, delta)
                else:
                    # Extraneous file, remove
                    delta = []
                    _clean(join(basepath,fpath), delta.append)
                    if delta:
                        delta = sum(delta)
                        self.size -= delta
                        fullsize -= delta
                        excess -= delta
                        logger.debug('Removed extraneous file %r of size %d', fpath, delta)

        if expirations:
            for bpath in expirations:
                delta = []
                for suffix in ('.key','.file','.raw','.ser'):
                    _clean(bpath+suffix, delta.append)
                if delta:
                    delta = sum(delta)
                    self.size -= delta
                    excess -= delta
                    fullsize -= delta
                    expired_items += 1
                    expired_bytes += delta
                    logger.debug('Removed stale entry of size %d at %r', delta, bpath)

        if deletions:
            while deletions and excess <= -deletions[0][2]:
                delitem = heapq.heappop(deletions)
                excess += delitem[2]
            for atime,bpath,itemsize in deletions:
                delta = []
                for suffix in ('.file','.raw','.ser','.key'):
                    _clean(bpath+suffix, delta.append)
                if delta:
                    delta = sum(delta)
                    self.size -= delta
                    fullsize -= delta
                    evicted_items += 1
                    evicted_bytes += delta
                    logger.debug('Removed overflow entry of size %d LRU %fs ago at %r', 
                        delta, time.time() - atime, bpath)
        
        self._failfast_cache.clear()

        logger.info('Expired %d bytes in %d items', expired_bytes, expired_items)
        logger.info('Evicted %d bytes in %d items', evicted_bytes, evicted_items)

        # We've been computing the actual size, so adjust the approximation
        adjustment = fullsize - int(self.size)
        if adjustment != 0:
            self.size += adjustment
            logger.info('Adjusted size discrepancy of %+d (final %d)', adjustment, int(self.size))
        
