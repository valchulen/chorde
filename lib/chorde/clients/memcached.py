# -*- coding: utf-8 -*-
import zlib
import itertools
import hashlib
import memcache

from base_cache import BaseCacheClient, NONE

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


class MemcachedClient(BaseCacheClient):
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

    @property
    def async(self):
        return False
    
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
    
    def get(self, key, default=NONE):
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
    
    def clear(self):
        # We don't want to clear memcache, it might be shared
        pass

    def purge(self):
        # Memcache does that itself
        pass
    
    def contains(self, key):
        # Exploit the fact that append returns True on success (the key exists)
        # and False on failure (the key doesn't exist), with minimal bandwidth
        short_key = self.shorten_key(key)
        return self.client.append(short_key+"|0","")
        
