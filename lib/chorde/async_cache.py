from serialize import serialize
import zlib
import logging
import time
import weakref

# No need for real multiprocessing. In fact, using real
# multiprocessing would force pickling of values, which would be
# undesirable, pushing pickling cost into foreground threads.
from multiprocessing.pool import ThreadPool
from threading import Event

try:
    import cPickle
except ImportError:
    import pickle as cPickle

import sPickle

try:
    import json
except ImportError:
    json = None
JSON_SEPARATORS = (',',':')


class _NONE:pass
class _DELETE:pass

class AsyncCacheWriterPool(ThreadPool):
    def __init__(self, size, workers, client):
        self.client = client
        self.logger = logging.getLogger("AsyncCache")
        self.size = size
        self.workers = workers
        
        # queueset holds the values to be written, associated
        # by key, providing some write-back coalescense in
        # high-load environments
        self.queueset = {}
        self.done_event = Event()
        
        ThreadPool.__init__(self, workers)

    def _wait_done(self, timeout=None):
        ev = self.done_event
        if timeout is not None:
            ev.wait(timeout)
            if ev.isSet():
                ev.clear()
        else:
            ev.wait()
            ev.clear()

    @staticmethod
    def _writer(self, key, ttl):
        # self is weakref
        self = self()
        if self is None:
            return

        ev = self.done_event
        value = self.dequeue(key)

        try:
            if value is _NONE:
                # Something's hinky
                return
            
            elif value is _DELETE:
                try:
                    self.client.delete(key)
                except:
                    self.logger.error("Error deleting key", exc_info=True)
            
            else:
                try:
                    self.client.put(key, value, ttl)
                except:
                    self.logger.error("Error saving data in cache", exc_info=True)
        finally:
            # Signal waiting threads
            ev.set()
        
    @serialize
    def dequeue(self, key):
        return self.queueset.pop(key, _NONE)

    def enqueue(self, key, value, ttl=None):
        while len(self.queueset) >= self.size:
            self._wait_done(1.0)
        self._enqueue(key, value, ttl)
    
    @serialize
    def _enqueue(self, key, value, ttl):
        if key not in self.queueset:
            self.queueset[key] = value
            self.apply_async(self._writer, (weakref.ref(self), key, ttl))
        else:
            self.queueset[key] = value
    
    def waitkey(self, key, timeout=None):
        if timeout is None:
            while key in self.queueset:
                self._wait_done(1.0)
        else:
            tfin = time.time() + timeout
            while key in self.queueset and tfin >= time.time():
                self._wait_done(max(1.0, timeout))
                timeout = tfin - time.time()
    
    def get(self, key, default = None):
        return self.queueset.get(key, default)
    
    def contains(self, key):
        return key in self.queueset

    def put(self, key, value, ttl):
        self.enqueue(key, value, ttl)
    
    def delete(self, key):
        self.enqueue(key, _DELETE)
    
class MemcachedClient(object):
    def __init__(self, 
            client_addresses, 
            max_backing_key_length = 250,
            max_backing_value_length = 1000*1024,
            pickler = None,
            namespace = None,
            checksum_key = None, # CHANGE IT!
            **client_args):
        if checksum_key is None:
            raise ValueError, "MemcachedClient requires a checksum key for security checks"
        
        try:
            import memcache
        except ImportError:
            raise NotImplementedError, "There's no suitable external cache implementation"
        
        # make room for the hash prefix
        max_backing_key_length -= len(sPickle.checksum_algo_name) + 1
        
        max_backing_key_length = min(
            max_backing_key_length,
            memcache.SERVER_MAX_KEY_LENGTH)
        
        self.max_backing_key_length = max_backing_key_length - 16 # 16-bytes for page suffix
        self.max_backing_value_length = max_backing_value_length - 256 # 256-bytes for page header and other overhead
        self.last_seen_stamp = 0
        self.pickler = pickler or cPickle
        self.namespace = namespace
        
        if self.namespace:
            self.max_backing_key_length -= len(self.namespace)+1
        
        assert self.max_backing_key_length > 48
        assert self.max_backing_value_length > 128
        
        if 'pickleProtocol' not in client_args:
            # use binary protocol, otherwise binary data gets inflated
            # unreasonably when pickling
            client_args['pickleProtocol'] = 2
        
        if 'pickler' not in client_args:
            client_args['pickler'] = lambda *p, **kw: sPickle.SecurePickler(checksum_key, *p, **kw)
        
        if 'unpickler' not in client_args:
            client_args['unpickler'] = lambda *p, **kw: sPickle.SecureUnpickler(checksum_key, *p, **kw)
        
        self.client = memcache.Client(client_addresses, **client_args)
    
    def shorten_key(self, key):
        # keys cannot be anything other than strings
        if not isinstance(key, basestring):
            try:
                # Try JSON
                key = "J#"+json.dumps(key, separators=JSON_SEPARATORS)
            except:
                # Try pickling
                key = "P#"+self.pickler.dumps(key,2).encode("base64").replace("\n","")
        elif isinstance(key, unicode):
            key = "U#" + key.encode("utf-8")

        # keys cannot contain control characters or spaces
        ord_ = ord
        for c in key:
            if ord_(c) < 33 or ord_(c) == 127:
                key = "B#" + key.encode("base64").replace("\n","")
                break
        
        if len(key) > self.max_backing_key_length:
            # keys cannot be too long, accept the possibility of collision,
            # and shorten it by truncating and perhaps appending an MD5 hash.
            try:
                import hashlib
                key = "H%s#%s" % (hashlib.md5(key).digest().encode("hex"),key[:self.max_backing_key_length-48])
            except ImportError:
                key = "H%08X#%s" % (hash(key), key[:self.max_backing_key_length-16])
        
        if not key:
            key = "#NULL#"
        
        if self.namespace:
            key = "%s|%s" % (self.namespace,key)
        
        return "%s,%s" % (sPickle.checksum_algo_name, key)
    
    def get_version_stamp(self):
        stamp_key = "#--version-counter--#"
        stamp = self.client.incr(stamp_key)
        if stamp is None:
            import random
            self.client.add(stamp_key, 
                self.last_seen_stamp + 100 + int(random.random() * 1000) )
            stamp = self.client.incr(stamp_key) or 0
        self.last_seen_stamp = stamp
        return stamp
    
    def encode_pages(self, key, value):
        # Always pickle & compress, since we'll always unpickle.
        # Note: compress with very little effort (level=1), 
        #   otherwise it's too expensive and not worth it
        value = zlib.compress( self.pickler.dumps((key,value),2), 1 )
        
        npages = (len(value) + self.max_backing_value_length - 1) / self.max_backing_value_length
        pagelen = self.max_backing_value_length
        version = self.get_version_stamp()
        page = 0
        for page,start in enumerate(xrange(0,len(value),self.max_backing_value_length)):
            yield (npages, page, version, value[start:start+pagelen])
        
        assert page == npages-1
    
    def decode_pages(self, pages, canclear=True):
        if 0 not in pages:
            raise ValueError, "Missing page"
        
        ref_npages, _, ref_version, _ = pages[0]
        data = [None] * ref_npages
        
        for pageno, (npages, page, version, pagedata) in pages.iteritems():
            if (    pageno != page 
                 or version != ref_version 
                 or npages != ref_npages 
                 or not (0 <= page < ref_npages) 
                 or data[page] is not None
                 or not isinstance(pagedata,str) ):
                raise ValueError, "Inconsistent data in cache"
            data[page] = pagedata
        
        # free up memory if possible
        if canclear:
            pages.clear()
        
        # join pages, decompress, unpickle
        data = ''.join(data)
        data = zlib.decompress(data)
        data = self.pickler.loads(data)
        
        return data
    
    def get(self, key, default=_NONE):
        # get the first page (gambling that most entries will span only a single page)
        # then query for the remaining ones in a single roundtrip, if present,
        # for a combined total of 2 roundtrips.
        short_key = self.shorten_key(key)
        
        pages = { 0 : self.client.get(short_key+"|0") }
        if pages[0] is None or not isinstance(pages[0],tuple) or len(pages[0]) != 4:
            return default
        
        npages = pages[0][0]
        if npages > 1:
            pages.update( self.client.get_multi(xrange(1,npages), key_prefix=short_key+"|") )
        
        try:
            cached_key, cached_value = self.decode_pages(pages)
            
            if cached_key == key:
                return cached_value
            else:
                return default
        except ValueError:
            return default
        except:
            self.logger.warning("Error decoding cached data", exc_info=True)
            return default
        
    
    def put(self, key, value, ttl):
        # set_multi all pages in one roundtrip
        short_key = self.shorten_key(key)
        pages = dict([(page,data) for page,data in enumerate(self.encode_pages(key, value))])
        self.client.set_multi(pages, ttl, key_prefix=short_key+"|")
    
    def delete(self, key):
        # delete the first page (gambling that most entries will span only a single page)
        # then query for the second, and if present, delete all the other pages
        # in a single roundtrip, for a combined total of 3 roundtrips.
        short_key = self.shorten_key(key)
        self.client.delete(short_key+"|0")
        
        page = self.client.get(short_key+"|1")
        if page is not None:
            npages = page[0]
            del page # big structure, free ASAP
            
            self.client.delete_multi(xrange(1,npages), key_prefix=short_key+"|")
    
    def contains(self, key):
        # Exploit the fact that append returns True on success (the key exists)
        # and False on failure (the key doesn't exist), with minimal bandwidth
        short_key = self.shorten_key(key)
        return self.client.append(short_key+"|0","")
        

class AsyncCacheClient(object):
    def __init__(self, 
            client_addresses,
            writer_queue_size, writer_workers, 
            **client_args ):
        if 'client_object' in client_args:
            # If given a client object explicitly, use it
            self.client = client_args['client_object']
        else:
            # Otherwise, search for a suitable external cache implementation
            # ATM: only memcache
            try:
                self.client = MemcachedClient(client_addresses, **client_args)
            except NotImplementedError:
                # No more implementations available
                raise
        
        self.writer_queue_size = writer_queue_size
        self.writer_workers = writer_workers
        
        self.writer = None
        
    def assert_started(self):
        if self.writer is None:
            self.writer = AsyncCacheWriterPool(
                self.writer_queue_size, 
                self.writer_workers,
                self.client)
    
    def is_started(self):
        return self.writer is not None
    
    def start(self):
        if self.writer is not None:
            raise AssertionError, "Starting AsyncCacheClient twice"
        self.assert_started()
    
    def stop(self, abort_tasks=False):
        if self.writer is not None:
            if not abort_tasks:
                self.writer.join()
            self.writer.terminate()
    
    def put(self, key, value, ttl):
        self.assert_started()
        self.writer.put(key, value, ttl)
    
    def delete(self, key):
        self.assert_started()
        self.writer.delete(key)
    
    def get(self, key):
        if self.is_started():
            # Try to read pending writes as if they were on the cache
            value = self.writer.get(key, _NONE)
            if value is not _NONE:
                return value
        
        # Ok, read the cache then
        value = self.client.get(key, _NONE)
        if value is _NONE:
            raise KeyError, key
        else:
            return value
    
    def contains(self, key):
        if self.is_started():
            if self.writer.contains(key):
                return True
            else:
                return self.client.contains(key)
        else:
            return self.client.contains(key)
    


