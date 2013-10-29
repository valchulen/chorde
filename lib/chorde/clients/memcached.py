# -*- coding: utf-8 -*-
import collections
import hashlib
import itertools
import logging
import memcache
import random
import time
import weakref
import zlib
from threading import Event, Thread

from .base import BaseCacheClient, CacheMissError, NONE
from .inproc import Cache

try:
    import cPickle
except ImportError:
    import pickle as cPickle

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

from chorde import sPickle

try:
    import json
except ImportError:
    json = None
JSON_SEPARATORS = (',',':')

class ZlibFile:
    def __init__(self, fileobj, level = 9):
        self.fileobj = fileobj
        self.compressor = zlib.compressobj(level)
        self.level = level
        self.flushed = True
        self.closed = False

    def write(self, buf):
        self.fileobj.write(self.compressor.compress(buf))
        self.flushed = False

    def flush(self):
        if not self.flushed:
            self.fileobj.write(self.compressor.flush())
            self.flushed = True
        self.fileobj.flush()

    def close(self):
        if not self.closed:
            self.flush()
            self.closed = True

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

class MemcachedClient(BaseCacheClient):
    def __init__(self, 
            client_addresses, 
            max_backing_key_length = 250,
            max_backing_value_length = 1000*1024,
            failfast_size = 100,
            failfast_time = 0.1,
            pickler = None,
            namespace = None,
            checksum_key = None, # CHANGE IT!
            **client_args):
        if checksum_key is None:
            raise ValueError, "MemcachedClient requires a checksum key for security checks"
        
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
        self.failfast_time = failfast_time
        
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

        self._client_args = client_args
        self._client_addresses = client_addresses
        self._client = None
        
        self._failfast_cache = Cache(failfast_size)

    @property
    def client(self):
        if self._client is None:
            self._client = memcache.Client(self._client_addresses, **self._client_args)
        return self._client

    @property
    def async(self):
        return False
    
    @property
    def stats(self):
        stats = getattr(self, '_stats', None)
        if stats is None or stats[1] < time.time():
            stats = collections.defaultdict(int)
            for srv,s in self.client.get_stats():
                for k,v in s.iteritems():
                    try:
                        v = int(v)
                        stats[k] += v
                    except:
                        pass
            self._stats = (stats, time.time() + 1)
        else:
            stats = stats[0]
        return stats
    
    @property
    def capacity(self):
        return self.stats.get('limit_maxbytes', 0)

    @property
    def usage(self):
        return self.stats.get('bytes', 0)

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
        for c in itertools.imap(ord,key):
            if c < 33 or c == 127:
                key = "B#" + key.encode("base64").replace("\n","")
                break
        
        if len(key) > self.max_backing_key_length:
            # keys cannot be too long, accept the possibility of collision,
            # and shorten it by truncating and perhaps appending an MD5 hash.
            try:
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
        try:
            stamp = self.client.incr(stamp_key)
        except ValueError:
            # Sometimes shit happens when there's memory pressure, we lost the stamp
            # Some other times, the client gets borked
            logging.warn("MemcachedClient: Error reading version counter, resetting client")
            self._client = None
            try:
                stamp = self.client.incr(stamp_key)
            except ValueError:
                stamp = None
        if stamp is None:
            stamp = self.last_seen_stamp + 100 + int(random.random() * 1000)
            self.client.add(stamp_key, stamp )
            try:
                stamp = self.client.incr(stamp_key) or 0
            except ValueError:
                # Again, this is fucked up
                logging.warn("MemcachedClient: Error again reading version counter")
                pass
        self.last_seen_stamp = stamp
        return stamp
    
    def encode_pages(self, key, ttl, value):
        # Always pickle & compress, since we'll always unpickle.
        # Note: compress with very little effort (level=1), 
        #   otherwise it's too expensive and not worth it
        sio = StringIO()
        with ZlibFile(sio, 1) as zio:
            self.pickler.dump((key,value),zio,2)
        value = sio.getvalue()
        sio.close()
        del sio,zio
        
        npages = (len(value) + self.max_backing_value_length - 1) / self.max_backing_value_length
        pagelen = self.max_backing_value_length
        version = self.get_version_stamp()
        page = 0
        for page,start in enumerate(xrange(0,len(value),self.max_backing_value_length)):
            yield (npages, page, ttl, version, value[start:start+pagelen])
        
        assert page == npages-1

    def decode_pages(self, pages, canclear=True):
        if 0 not in pages:
            raise ValueError, "Missing page"
        
        ref_npages, _, ref_ttl, ref_version, _ = pages[0]
        data = [None] * ref_npages
        
        for pageno, (npages, page, ttl, version, pagedata) in pages.iteritems():
            if (    pageno != page 
                 or version != ref_version 
                 or npages != ref_npages 
                 or ttl != ref_ttl
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
    
    def _getTtl(self, key, default, decode = True, ttlcut = None):
        now = time.time()
        
        # get the first page (gambling that most entries will span only a single page)
        # then query for the remaining ones in a single roundtrip, if present,
        # for a combined total of 2 roundtrips.
        short_key = self.shorten_key(key)
        
        pages = { 0 : self.client.get(short_key+"|0") }
        if pages[0] is None or not isinstance(pages[0],tuple) or len(pages[0]) != 5:
            return default, -1
        
        ttl = pages[0][2]
        npages = pages[0][0]

        if not decode:
            return default, ttl - now
        elif ttlcut is not None and ttl < ttlcut:
            return default, -1
        # Check failfast cache, before making a huge effort decoding for not
        # When there's a key collision, this avoids misses being expensive
        elif self._failfast_cache.get(key) > (now - self.failfast_time):
            return default, -1
        
        if npages > 1:
            pages.update( self.client.get_multi(xrange(1,npages), key_prefix=short_key+"|") )
        
        try:
            cached_key, cached_value = self.decode_pages(pages)
            
            if cached_key == key:
                return cached_value, ttl - now
            else:
                self._failfast_cache[key] = now
                return default, -1
        except ValueError:
            self._failfast_cache[key] = now
            return default, -1
        except:
            self._failfast_cache[key] = now
            logging.warning("Error decoding cached data", exc_info=True)
            return default, -1

    def getTtl(self, key, default=NONE):
        # This trampoline is necessary to avoid re-entrancy issues when this client
        # is wrapped inside a SyncWrapper. Internal calls go directly to _getTtl
        # to avoid locking the wrapper's mutex.
        return self._getTtl(key, default)

    def get(self, key, default=NONE):
        rv, ttl = self._getTtl(key, default, ttlcut = 0)
        if ttl < 0 and default is NONE:
            raise CacheMissError, key
        else:
            return rv
    
    def put(self, key, value, ttl):
        # set_multi all pages in one roundtrip
        short_key = self.shorten_key(key)
        pages = dict([(page,data) for page,data in enumerate(self.encode_pages(key, ttl+time.time(), value))])
        self.client.set_multi(pages, ttl, key_prefix=short_key+"|")
        
        try:
            del self._failfast_cache[key]
        except:
            pass
    
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
    
    def clear(self):
        # We don't want to clear memcache, it might be shared
        pass

    def purge(self):
        # Memcache does that itself
        pass
    
    def contains(self, key, ttl = None):
        # Exploit the fact that append returns True on success (the key exists)
        # and False on failure (the key doesn't exist), with minimal bandwidth
        short_key = self.shorten_key(key)
        exists = self.client.append(short_key+"|0","")
        if exists:
            if ttl is None:
                try:
                    del self._failfast_cache[key]
                except:
                    pass
                return True
            else:
                # Checking with a TTL margin requires some extra care, because
                # pages can be very expensive to decode, we first only fetch
                # the TTL in the first page, and validate pessimistically.
                # When checking with a TTL margin a key that's stale, this will
                # minimize bandwidth, but when it's valid, it will result in
                # 3x roundtrips: check with append, get ttl, get key
                
                # check TTL quickly, no decoding (or fetching) of pages needed
                # to check stale TTL
                _, store_ttl = self._getTtl(key, NONE, False)
                if store_ttl <= ttl:
                    return False
                else:
                    # Must validate the key, so we must decode
                    rv, store_ttl = self._getTtl(key, NONE)
                    if rv is NONE:
                        # wrong key
                        return False
                    else:
                        try:
                            del self._failfast_cache[key]
                        except:
                            pass
                        return True
        else:
            return False

_memcache_writer_sequence = 1
class MemcacheWriterThread(Thread):
    def __init__(self, target, *args):
        global _memcache_writer_sequence
        # no need to make atomic huh? just a name...
        threadno = _memcache_writer_sequence
        _memcache_writer_sequence += 1

        name = 'memcached-writer-thread-%d' % (threadno,)
        
        Thread.__init__(self, target=target, args=args, name=name)

class FastMemcachedClient(BaseCacheClient):
    """
    Like MemcachedClient, but it doesn't support massive keys or values,
    is a lot more lightweight, and is optimized for high numbers of writes
    and reads rather than thoughput with big values. It also uses json
    serialization so it supports only a subset of python types, but it then
    requires no checksum key and is thus faster.

    Params:
        key_pickler: specify a json-like implementation. It must support loads
            and dumps, and dumps must take a "separators" keyword argument
            just as stdlib json.dumps does, but it doesn't need to honour it
            aside from not generating control chars (spaces). 
            You can use cjson for instance, but you'll have to wrap it as it
            doesn't support separators and it will generate spaces. 
            Do not pass Pickle or its other  implementations, as it will work, 
            but the pickle protocol isn't  secure. Use MemcachedClient 
            if you need the features of pickling.
        pickler: specify a json-like implementation. Like key_pickler, except
            it can completely dishonor separators without major issues, as
            spaces and control chars will be accepted for values. If not
            specified, key_pickler will be used.
    """
    
    def __init__(self, 
            client_addresses, 
            max_backing_key_length = 250,
            max_backing_value_length = 1000*1024,
            key_pickler = None,
            pickler = None,
            namespace = None,
            **client_args):
        
        max_backing_key_length = min(
            max_backing_key_length,
            memcache.SERVER_MAX_KEY_LENGTH)

        if key_pickler is None:
            key_pickler = json
        
        self.max_backing_key_length = max_backing_key_length 
        self.max_backing_value_length = max_backing_value_length - 32 # 32-bytes for various overheads
        self.key_pickler = key_pickler
        self.pickler = pickler or key_pickler
        self.namespace = namespace
        
        if self.namespace:
            self.max_backing_key_length -= len(self.namespace)+1
        
        assert self.max_backing_key_length > 48
        assert self.max_backing_value_length > 128
        
        self._client_args = client_args
        self._client_addresses = client_addresses
        self._client = None
        
        self.queueset = {}
        self.workset = {}
        self.workev = Event()

        self._bgwriter_thread = MemcacheWriterThread(self._bgwriter, weakref.ref(self))
        self._bgwriter_thread.setDaemon(True)

    @property
    def client(self):
        if self._client is None:
            self._client = memcache.Client(self._client_addresses, **self._client_args)
        return self._client

    @property
    def async(self):
        return False
    
    @property
    def stats(self):
        stats = getattr(self, '_stats', None)
        if stats is None or stats[0] < time.time():
            stats = self.client.get_stats() or {}
            self._stats = (stats, time.time() + 1)
        return stats
    
    @property
    def capacity(self):
        return self.stats.get('limit_maxbytes', 0)

    @property
    def usage(self):
        return self.stats.get('bytes', 0)

    @property
    def queuelen(self):
        return len(self.queueset)

    def _enqueue_put(self, key, value, ttl):
        # Atomic insert
        value = value, ttl
        self.queueset[key] = value
        self.workev.set()

        if not self._bgwriter_thread.isAlive():
            try:
                self._bgwriter_thread.start()
            except:
                # Might have been started by other thread
                pass

    def _dequeue_put(self):
        # Almost-Atomic swap, time.sleep after extracting and it's fully-atomic
        # as other threads don't keep references to queueset for long,
        # it does require only one consumer thread though
        self.workset, self.queueset = self.queueset, self.workset
        time.sleep(0)
        return self.workset

    @staticmethod
    def _bgwriter(wself):
        while True:
            self = wself()
            if self is None:
                break

            workev = self.workev
            workev.clear()
            workset = self._dequeue_put()

            # Separate into deletions, puttions, and group by ttl
            # since put_multi can only handle one ttl.
            # Deletions are value=NONE
            plan = collections.defaultdict(dict)
            deletions = []
            for i in xrange(2):
                # It can explode if a thread lingers, so restart if that happens
                try:
                    for key, (value, ttl) in workset.iteritems():
                        if value is NONE:
                            deletions.append(key)
                        else:
                            plan[ttl][key] = value
                    break
                except RuntimeError:
                    pass

            if deletions:
                try:
                    self.client.delete_multi(deletions)
                except:
                    logging.error("Exception in background writer", exc_info = True)
            if plan:
                for ttl, batch in plan.iteritems():
                    try:
                        self.client.set_multi(batch, ttl)
                    except:
                        logging.error("Exception in background writer", exc_info = True)
            workset.clear()
            
            # Let us be suicidal
            del self, plan, deletions, workset
            key = value = ttl = batch = None
            workev.wait(1)
    
    def encode_key(self, key):
        return self.key_pickler.dumps((self.namespace, key), separators = JSON_SEPARATORS)
    
    def encode(self, key, ttl, value):
        # Always pickle & compress, since we'll always unpickle.
        # Note: compress with very little effort (level=1), 
        #   otherwise it's too expensive and not worth it
        return self.pickler.dumps((value, ttl), separators = JSON_SEPARATORS)

    def decode(self, value):
        value, ttl = self.pickler.loads(value)
        return value, ttl
    
    def _getTtl(self, key, default):
        key = self.encode_key(key)
        
        # Quick check for a concurrent put
        value = self.queueset.get(key, self.workset.get(key, NONE))
        if value is NONE:
            value = self.client.get(key)
        else:
            value = value[0]
            if value is NONE:
                # A deletion is queued, so we deleted
                value = None
        
        if value is None:
            return default, -1
        
        try:
            value, ttl = self.decode(value)
            return value, ttl - time.time()
        except ValueError:
            return default, -1
        except:
            logging.warning("Error decoding cached data (%r)", value, exc_info=True)
            return default, -1

    def getTtl(self, key, default=NONE):
        # This trampoline is necessary to avoid re-entrancy issues when this client
        # is wrapped inside a SyncWrapper. Internal calls go directly to _getTtl
        # to avoid locking the wrapper's mutex.
        return self._getTtl(key, default)
    
    def put(self, key, value, ttl):
        # set_multi all pages in one roundtrip
        key = self.encode_key(key)
        value = self.encode(key, ttl+time.time(), value)
        self._enqueue_put(key, value, ttl)
    
    def add(self, key, value, ttl):
        # set_multi all pages in one roundtrip
        key = self.encode_key(key)

        if self.queueset.get(key, NONE) is not NONE:
            return False
            
        value = self.encode(key, ttl+time.time(), value)
        rv = self.client.add(key, value, ttl)

        if rv is True:
            return True
        elif rv is False:
            return False
        else:
            raise RuntimeError, "Memcache add returned %r" % (rv,)
    
    def delete(self, key):
        key = self.encode_key(key)
        self._enqueue_put(key, NONE, 0)
    
    def clear(self):
        # We don't want to clear memcache, it might be shared
        pass

    def purge(self):
        # Memcache does that itself
        pass
    
    def contains(self, key, ttl = None):
        key = self.encode_key(key)

        # Quick check against worker queues
        if key in self.queueset or key in self.workset:
            return True

        # Else exploit the fact that append returns True on success (the key exists)
        # and False on failure (the key doesn't exist), with minimal bandwidth
        exists = self.client.append(key,"")
        if exists:
            if ttl is None:
                return True
            else:
                # Checking with a TTL margin requires some extra care.
                # When checking with a TTL margin a key that's stale, this will
                # minimize bandwidth, but when it's valid, it will result in
                # 2x roundtrips: check with append, get ttl
                _, store_ttl = self._getTtl(key, NONE)
                return store_ttl > ttl
        else:
            return False
