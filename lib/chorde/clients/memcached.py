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
from threading import Event, Thread, Lock

from .base import BaseCacheClient, CacheMissError, NONE
from .inproc import Cache

_RENEW = object()

STATS_CACHE_TIME = 1

try:
    import cPickle
except ImportError:
    import pickle as cPickle  # lint:ok

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO  # lint:ok

from chorde import sPickle
from chorde.dnsutils import DynamicResolvingClient

try:
    try:
        import json
    except ImportError:
        import simplejson as json  # lint:ok
except ImportError:
    json = None  # lint:ok
JSON_SEPARATORS = (',',':')

try:
    try:
        import ujson as cjson_
    except ImportError:
        import cjson as cjson_  # lint:ok
    class cjson:  # lint:ok
        loads = cjson_.decode
        dumps = staticmethod(lambda x, separators=None, encode = cjson_.encode : encode(x))
except ImportError:
    try:
        import simplejson as cjson  # lint:ok
    except ImportError:
        import json as cjson  # lint:ok

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

class DynamicResolvingMemcachedClient(BaseCacheClient, DynamicResolvingClient):
    def __init__(self, client_class, client_addresses, client_args):
        super(DynamicResolvingMemcachedClient, self).__init__(
                client_class, client_addresses, client_args)

class MemcachedClient(DynamicResolvingMemcachedClient):
    def __init__(self, 
            client_addresses, 
            max_backing_key_length = 250,
            max_backing_value_length = 1000*1024,
            failfast_size = 100,
            failfast_time = 0.1,
            succeedfast_size = 10,
            succeedfast_time = 0.25,
            pickler = None,
            namespace = None,
            compress = True,
            checksum_key = None, # CHANGE IT!
            encoding_cache = None, # should be able to contain attributes
            client_class = memcache.Client,
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
        self.succeedfast_time = succeedfast_time
        self.encoding_cache = encoding_cache
        self.compress = compress
        
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

        self._failfast_cache = Cache(failfast_size)
        self._succeedfast_cache = Cache(succeedfast_size)

        super(MemcachedClient, self).__init__(client_class, client_addresses, client_args)

    @property
    def async(self):
        return False
    
    @property
    def stats(self):
        stats = getattr(self, '_stats', None)
        if stats is None or stats[1] < time.time():
            stats = collections.defaultdict(int)
            try:
                client_stats = self.client.get_stats()
            except:
                logging.warn("MemcachedClient: Error getting stats, resetting client and retrying")
                del self.client
                client_stats = self.client.get_stats()
            for srv,s in client_stats:
                for k,v in s.iteritems():
                    try:
                        v = int(v)
                        stats[k] += v
                    except:
                        pass
            self._stats = (stats, time.time() + STATS_CACHE_TIME)
        else:
            stats = stats[0]
        return stats
    
    @property
    def capacity(self):
        return self.stats.get('limit_maxbytes', 0)

    @property
    def usage(self):
        return self.stats.get('bytes', 0)

    def shorten_key(self, key,
            tmap = ''.join('\x01' if c<33 or c == 127 else '\x00' for c in xrange(256)),
            imap = itertools.imap,
            isinstance = isinstance, basestring = basestring, unicode = unicode, ord = ord, any = any, len = len ):
        # keys cannot be anything other than strings
        exact = True
        zpfx = 'z#'
        if not isinstance(key, basestring):
            try:
                # Try JSON
                key = "J#"+json.dumps(key, separators=JSON_SEPARATORS)
                zpfx = 'z'
            except:
                # Try pickling
                key = "P#"+self.pickler.dumps(key,2).encode("base64").replace("\n","")
                zpfx = 'z'
        elif isinstance(key, unicode):
            key = "U#" + key.encode("utf-8")
            zpfx = 'z'

        # keys cannot contain control characters or spaces
        if any(imap(ord, key.translate(tmap))):
            key = "B#" + key.encode("base64").replace("\n","")
            zpfx = 'z'

        if self.compress:
            key = zpfx + key
        
        if len(key) > self.max_backing_key_length:
            # keys cannot be too long, accept the possibility of collision,
            # and shorten it by truncating and perhaps appending an MD5 hash.
            exact = False
            try:
                key = "H%s#%s" % (hashlib.md5(key).digest().encode("hex"),key[:self.max_backing_key_length-48])
            except ImportError:
                key = "H%08X#%s" % (hash(key), key[:self.max_backing_key_length-16])
        
        if not key:
            key = "#NULL#"
        
        if self.namespace:
            key = "%s|%s" % (self.namespace,key)
        
        return "%s,%s" % (sPickle.checksum_algo_name, key), exact
    
    def get_version_stamp(self):
        stamp_key = "#--version-counter--#"
        try:
            stamp = self.client.incr(stamp_key)
        except ValueError:
            # Sometimes shit happens when there's memory pressure, we lost the stamp
            # Some other times, the client gets borked
            logging.warn("MemcachedClient: Error reading version counter, resetting client")
            del self.client
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
        encoded = None
        if self.encoding_cache is not None:
            cached = getattr(self.encoding_cache, 'cache', None)
            if cached is not None and cached[0] is value:
                encoded = cached[1]
            del cached

        if encoded is None:
            # Always pickle & compress, since we'll always unpickle.
            # Note: compress with very little effort (level=1), 
            #   otherwise it's too expensive and not worth it
            sio = StringIO()
            if self.compress:
                with ZlibFile(sio, 1) as zio:
                    self.pickler.dump((key,value),zio,2)
                del zio
            else:
                self.pickler.dump((key,value),sio,2)
            encoded = sio.getvalue()
            sio.close()
            del sio

            if self.encoding_cache is not None:
                self.encoding_cache.cache = (value, encoded)
        
        npages = (len(encoded) + self.max_backing_value_length - 1) / self.max_backing_value_length
        pagelen = self.max_backing_value_length
        version = self.get_version_stamp()
        page = 0
        for page,start in enumerate(xrange(0,len(encoded),self.max_backing_value_length)):
            yield (npages, page, ttl, version, encoded[start:start+pagelen])
        
        assert page == npages-1

    def decode_pages(self, pages, key, canclear=True):
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

        if self.encoding_cache is not None:
            # Check against the cached encoding just in case it's the same
            # This way we avoid deserializing
            cached = getattr(self.encoding_cache, 'cache', None)
            if cached is not None and cached[1] == data:
                return (key,cached[0])
            del cached

        if self.compress:
            value = zlib.decompress(data)
        else:
            value = data
        value = self.pickler.loads(value)

        if self.encoding_cache is not None and isinstance(value, tuple) and len(value) > 1:
            self.encoding_cache.cache = (value[1], data)
        
        return value
    
    def _getTtl(self, key, default, decode = True, ttl_skip = None, short_key = None, pages = None, 
            method = None, multi_method = None,
            force_all_pages = False):
        now = time.time()

        if decode:
            # First, check the succeedfast, in case a recent _getTtl already fetched this
            # This is a necessary optimization when used in tiered architectures, since
            # promotion and other tiered operations tend to abuse of contains and getTtl,
            # creating lots of redundant roundtrips and decoding overhead
            cached = self._succeedfast_cache.get(key, NONE)
            if cached is not NONE:
                cached, cached_time = cached
                if cached_time > (now - self.succeedfast_time):
                    # Ok
                    cached, ttl = cached
                    return cached, ttl - now
        
        # get the first page (gambling that most entries will span only a single page)
        # then query for the remaining ones in a single roundtrip, if present,
        # for a combined total of 2 roundtrips.
        if short_key is None:
            short_key,exact = self.shorten_key(key)

        if method is None:
            method = self.client.get
            multi_method = self.client.get_multi

        if pages is None:
            pages = { 0 : method(short_key+"|0") }
        if pages[0] is None or not isinstance(pages[0],tuple) or len(pages[0]) != 5:
            return default, -1
        
        ttl = pages[0][2]
        npages = pages[0][0]

        if not decode:
            if force_all_pages and npages > 1:
                if multi_method is not None:
                    pages.update( multi_method(xrange(1,npages), key_prefix=short_key+"|") )
                else:
                    pages.update([ method("%s|%d" % (short_key,i)) for i in xrange(1,npages) ])
            return pages, ttl - now
        elif ttl_skip is not None and ttl < ttl_skip:
            return default, -1
        # Check failfast cache, before making a huge effort decoding for not
        # When there's a key collision, this avoids misses being expensive
        elif self._failfast_cache.get(key) > (now - self.failfast_time):
            return default, -1
        
        if npages > 1:
            if multi_method:
                pages.update( multi_method(xrange(1,npages), key_prefix=short_key+"|") )
            else:
                pages.update([ method("%s|%d" % (short_key,i)) for i in xrange(1,npages) ])
        
        try:
            cached_key, cached_value = self.decode_pages(pages, key)
            
            if cached_key == key:
                self._succeedfast_cache[key] = (cached_value, ttl), now
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

    def getTtl(self, key, default=NONE, ttl_skip = None):
        # This trampoline is necessary to avoid re-entrancy issues when this client
        # is wrapped inside a SyncWrapper. Internal calls go directly to _getTtl
        # to avoid locking the wrapper's mutex.
        return self._getTtl(key, default, ttl_skip = ttl_skip)

    def get(self, key, default=NONE):
        rv, ttl = self._getTtl(key, default, ttl_skip = 0)
        if ttl < 0 and default is NONE:
            raise CacheMissError, key
        else:
            return rv

    def renew(self, key, ttl):
        short_key,exact = self.shorten_key(key)
        raw_pages, store_ttl = self._getTtl(key, NONE, False, short_key = short_key, 
            method = self.client.gets)
        if raw_pages is not NONE and store_ttl < ttl:
            now = time.time()
            for i,page in raw_pages.iteritems():
                new_page = page[:2] + (max(ttl + now, page[2]),) + page[3:]
                success = self.client.cas("%s|%d" % (short_key,i), new_page, ttl)
                if success:
                    cached = self._succeedfast_cache.get(key, NONE)
                    if cached is not NONE:
                        (value, _), cached_time = cached
                        ncached = ((value, ttl + now), now)
                        self._succeedfast_cache.cas(key, cached, ncached)
    
    def put(self, key, value, ttl):
        # set_multi all pages in one roundtrip
        short_key,exact = self.shorten_key(key)
        pages = dict([(page,data) for page,data in enumerate(self.encode_pages(key, ttl+time.time(), value))])
        self.client.set_multi(pages, ttl, key_prefix=short_key+"|")
        
        try:
            del self._failfast_cache[key]
        except:
            pass
        
        try:
            del self._succeedfast_cache[key]
        except:
            pass
    
    def delete(self, key):
        # delete the first page (gambling that most entries will span only a single page)
        # then query for the second, and if present, delete all the other pages
        # in a single roundtrip, for a combined total of 3 roundtrips.
        short_key,exact = self.shorten_key(key)
        self.client.delete(short_key+"|0")
        
        page = self.client.get(short_key+"|1")
        if page is not None:
            npages = page[0]
            del page # big structure, free ASAP
            
            self.client.delete_multi(xrange(1,npages), key_prefix=short_key+"|")
        
        try:
            del self._succeedfast_cache[key]
        except:
            pass
    
    def clear(self):
        # We don't want to clear memcache, it might be shared
        self._failfast_cache.clear()
        self._succeedfast_cache.clear()

    def purge(self):
        # Memcache does that itself
        self._failfast_cache.clear()
        self._succeedfast_cache.clear()
    
    def contains(self, key, ttl = None):
        short_key,exact = self.shorten_key(key)
        if ttl is None:
            # Exploit the fact that append returns True on success (the key exists)
            # and False on failure (the key doesn't exist), with minimal bandwidth
            exists = self.client.append(short_key+"|0","")
        else:
            # But not for ttl checks, those need to check the contents
            exists = True
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
                # 1 roundtrip still: no check with append, get ttl, 
                # get key with cached result, although it will incur higher CPU costs
                
                # check TTL quickly, no decoding (or fetching) of pages needed
                # to check stale TTL
                raw_pages, store_ttl = self._getTtl(key, NONE, False, short_key = short_key)
                if store_ttl <= ttl:
                    return False
                elif exact:
                    return True
                else:
                    # Must validate the key, so we must decode
                    rv, store_ttl = self._getTtl(key, NONE, short_key = short_key, pages = raw_pages)
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

class FastMemcachedClient(DynamicResolvingMemcachedClient):
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

        failfast_time, failfast_size: (optional) If given, a small in-process
            cache of misses will be kept in order to avoid repeated queries
            to the remote cache. By default, it's disabled, since misses
            are quick enough. Note: the semantics of the failfast cache
            are slightly different from the regular MemcachedClient. In this
            case, the failfast cache will be queried before checking the
            key in the remote cache, which means writes from other processes
            will be invisible until the failfast_time elapses. Thus, make
            sure such delay is acceptable before making use of this cache.
    """
    
    def __init__(self, 
            client_addresses, 
            max_backing_key_length = 250,
            max_backing_value_length = 1000*1024,
            key_pickler = None,
            pickler = None,
            namespace = None,
            failfast_time = None,
            failfast_size = 100,
            client_class = memcache.Client,
            **client_args):
        
        max_backing_key_length = min(
            max_backing_key_length,
            memcache.SERVER_MAX_KEY_LENGTH)

        self.max_backing_key_length = max_backing_key_length 
        self.max_backing_value_length = max_backing_value_length - 32 # 32-bytes for various overheads
        self.key_pickler = key_pickler or json
        self.pickler = pickler or key_pickler or cjson
        self.namespace = namespace
        
        if self.namespace:
            self.max_backing_key_length -= len(self.namespace)+1
        
        assert self.max_backing_key_length > 48
        assert self.max_backing_value_length > 128
        
        self.queueset = {}
        self.workset = {}
        self.workev = Event()

        self.failfast_time = failfast_time
        self._failfast_cache = Cache(failfast_size) if failfast_time else None

        super(FastMemcachedClient, self).__init__(client_class, client_addresses, client_args)

        self._bgwriter_thread = None
        self._spawning_lock = Lock()

    @property
    def async(self):
        return False
    
    @property
    def stats(self):
        stats = getattr(self, '_stats', None)
        if stats is None or stats[1] < time.time():
            stats = collections.defaultdict(int)
            try:
                client_stats = self.client.get_stats()
            except:
                logging.warn("FastMemcachedClient: Error getting stats, resetting client and retrying")
                del self.client
                client_stats = self.client.get_stats()
            for srv,s in client_stats:
                for k,v in s.iteritems():
                    try:
                        v = int(v)
                        stats[k] += v
                    except:
                        pass
            self._stats = (stats, time.time() + STATS_CACHE_TIME)
        else:
            stats = stats[0]
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

        if not self._bgwriter_thread or not self._bgwriter_thread.isAlive():
            with self._spawning_lock:
                if not self._bgwriter_thread or not self._bgwriter_thread.isAlive():
                    if not self._bgwriter_thread or not self._bgwriter_thread.isAlive():
                        bgwriter_thread = MemcacheWriterThread(self._bgwriter, weakref.ref(self))
                        bgwriter_thread.setDaemon(True)
                        bgwriter_thread.start()
                        self._bgwriter_thread = bgwriter_thread

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
            renewals = []
            quicknow = time.time()
            for i in xrange(2):
                # It can explode if a thread lingers, so restart if that happens
                try:
                    for key, (value, ttl) in workset.iteritems():
                        key = self.encode_key(key)
                        if value is NONE:
                            deletions.append(key)
                        elif value is _RENEW:
                            renewals.append((key, ttl))
                        else:
                            plan[ttl][key] = self.encode(key, ttl+quicknow, value)
                    break
                except RuntimeError, e:
                    del deletions[:]
                    del renewals[:]
                    plan.clear()
                    last_error = e
            else:
                # Um...
                logging.error("Exception preparing plan: %r", last_error)
                plan = deletions = None
            last_error = None

            if plan or deletions or renewals:
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
                if renewals:
                    for key, ttl in renewals:
                        value = self.client.gets(key)
                        if value is not None:
                            value, kttl = self.decode(value)
                            nttl = ttl + quicknow
                            if kttl < nttl:
                                value = self.encode(key, nttl, value)
                                self.client.cas(key, value, ttl)
                
                # Let us be suicidal
                del self, plan, deletions, renewals
                workset.clear()
                del workset
            
            key = value = ttl = kttl = nttl = batch = None
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
    
    def _getTtl(self, key, default, ttl_skip = None, encoded = False, raw_key = None):
        # Quick check for a concurrent put
        if encoded:
            value = NONE
        else:
            value = self.queueset.get(key, self.workset.get(key, NONE))
        if value is NONE or value[0] is _RENEW:
            now = time.time()
            
            # Not in queue, get from memcached, and decode
            if not encoded:
                # Check failfast cache, before contacting the remote client
                if self._failfast_cache is not None and self._failfast_cache.get(key) > (now - self.failfast_time):
                    return default, -1

                raw_key = key
                key = self.encode_key(key)
            value = self.client.get(key)
            if value is not None:
                try:
                    value, ttl = self.decode(value)
                    ttl -= now
                except ValueError:
                    if self._failfast_cache is not None:
                        self._failfast_cache[raw_key] = now
                    return default, -1
                except:
                    logging.warning("Error decoding cached data (%r)", value, exc_info=True)
                    if self._failfast_cache is not None:
                        self._failfast_cache[raw_key] = now
                    return default, -1
            elif self._failfast_cache is not None:
                self._failfast_cache[raw_key] = now
        else:
            # In queue, so it's already in decoded form
            value, ttl = value
            if value is NONE:
                # A deletion is queued, so we deleted
                value = None
        
        if value is None:
            return default, -1
        
        if ttl_skip is not None and ttl < ttl_skip:
            return default, -1
        else:
            return value, ttl

    def getTtl(self, key, default = NONE, ttl_skip = None):
        # This trampoline is necessary to avoid re-entrancy issues when this client
        # is wrapped inside a SyncWrapper. Internal calls go directly to _getTtl
        # to avoid locking the wrapper's mutex.
        return self._getTtl(key, default, ttl_skip = ttl_skip)
    
    def put(self, key, value, ttl):
        # set_multi all pages in one roundtrip
        self._enqueue_put(key, value, ttl)

        # Not necessary anymore, queue checks will resolve it from now on
        if self._failfast_cache is not None:
            try:
                del self._failfast_cache[key]
            except:
                pass
        
    def renew(self, key, ttl):
        # check the work queue
        value = self.queueset.get(key, NONE)
        if value is not NONE and value[0] is not NONE:
            # No need to be atomic, user-visible behavior is unchanged by concurrency
            value, kttl = value
            if kttl < ttl:
                self.queueset[key] = (value[0], ttl)
        else:
            # set_multi all pages in one roundtrip
            self._enqueue_put(key, _RENEW, ttl)
        
    def add(self, key, value, ttl):
        # set_multi all pages in one roundtrip
        if self.queueset.get(key, NONE) is not NONE:
            return False
        elif self.workset.get(key, NONE) is not NONE:
            return False
        
        key = self.encode_key(key)
        value = self.encode(key, ttl+time.time(), value)
        rv = self.client.add(key, value, ttl)

        if rv is True:
            return True
        elif rv is False:
            return False
        else:
            raise RuntimeError, "Memcache add returned %r" % (rv,)
    
    def delete(self, key):
        self._enqueue_put(key, NONE, 0)

        # Not necessary anymore, queue checks will resolve it from now on
        if self._failfast_cache is not None:
            try:
                del self._failfast_cache[key]
            except:
                pass
    
    def clear(self):
        # We don't want to clear memcache, it might be shared
        # But we can purge our queueset
        self.queueset.clear()
        self.workset.clear()
        if self._failfast_cache is not None:
            self._failfast_cache.clear()

    def purge(self, timeout = 0):
        # Memcache does that itself
        if self._failfast_cache is not None:
            self._failfast_cache.clear()
    
    def contains(self, key, ttl = None):
        # Quick check against worker queues
        if key in self.queueset or key in self.workset:
            return True

        # Check failfast cache, before contacting the remote client
        if self._failfast_cache is not None and self._failfast_cache.get(key) > (time.time() - self.failfast_time):
            return False
        
        # Else exploit the fact that append returns True on success (the key exists)
        # and False on failure (the key doesn't exist), with minimal bandwidth
        encoded_key = self.encode_key(key)
        exists = self.client.append(encoded_key,"")
        if exists:
            if ttl is None:
                return True
            else:
                # Checking with a TTL margin requires some extra care.
                # When checking with a TTL margin a key that's stale, this will
                # minimize bandwidth, but when it's valid, it will result in
                # 2x roundtrips: check with append, get ttl
                _, store_ttl = self._getTtl(encoded_key, NONE, encoded = True, raw_key = key)
                return store_ttl > ttl
        else:
            return False
