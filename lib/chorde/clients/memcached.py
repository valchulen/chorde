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
import socket
import select
from threading import Event, Thread, Lock

from .base import BaseCacheClient, CacheMissError, NONE
from .inproc import Cache

_RENEW = object()

STATS_CACHE_TIME = 1
# memcache doesn't allow TTL bigger than 2038
MAX_MEMCACHE_TTL = 0x7FFFFFFF - 1

try:
    import cPickle
except ImportError:
    import pickle as cPickle  # lint:ok

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO  # lint:ok

try:
    from select import poll
except ImportError:
    pass

from chorde import sPickle
from chorde.dnsutils import ThreadLocalDynamicResolvingClient

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

default_compression_pfx = zlib_compress_prefix = 'z'
default_compress_file_class = zlib_compress_file_class = ZlibFile
default_decompress_fn = zlib_decompress_fn = zlib.decompress

stamp_prefix = "#vc#"

try:
    import lz4
    
    class LZ4File:
        def __init__(self, fileobj, level = 9):
            self.fileobj = fileobj
            self.buffer = StringIO()
            self.flushed = True
            self.closed = False
    
        def write(self, buf):
            self.buffer.write(buf)
            self.flushed = False
    
        def flush(self):
            if not self.flushed:
                self.fileobj.write(lz4.compress(self.buffer.getvalue()))
                self.buffer.reset()
                self.buffer.truncate()
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

    lz4_compress_prefix = 'lz4'
    lz4_compress_file_class = LZ4File
    lz4_decompress_fn = lz4.decompress
except:
    lz4_compress_prefix = None
    lz4_compress_file_class = None
    lz4_decompress_fn = None

class MemcachedStoreClient(memcache.Client):
    # A faster check_key
    def check_key(self, key, key_extra_len=0,
            isinstance = isinstance, tuple = tuple, str = str, 
            unicode = unicode, basestring = basestring, len = len,
            tmap = ''.join('\x01' if c<33 or c == 127 else '\x00' for c in xrange(256)),
            imap = itertools.imap):
        """Checks sanity of key.  Fails if:
            Key length is > SERVER_MAX_KEY_LENGTH (Raises MemcachedKeyLength).
            Contains control characters  (Raises MemcachedKeyCharacterError).
            Is not a string (Raises MemcachedStringEncodingError)
            Is an unicode string (Raises MemcachedStringEncodingError)
            Is not a string (Raises MemcachedKeyError)
            Is None (Raises MemcachedKeyError)
        """
        if isinstance(key, tuple): key = key[1]
        if not key:
            raise self.MemcachedKeyNoneError("Key is None")
        if not isinstance(key, str):
            if isinstance(key, unicode):
                raise self.MemcachedStringEncodingError(
                        "Keys must be str()'s, not unicode.  Convert your unicode "
                        "strings using mystring.encode(charset)!")
            else:
                raise self.MemcachedKeyTypeError("Key must be str()'s")

        if self.server_max_key_length != 0 and \
            len(key) + key_extra_len > self.server_max_key_length:
            raise self.MemcachedKeyLengthError("Key length is > %s"
                     % self.server_max_key_length)
        if any(imap(ord, key.translate(tmap))):
            raise self.MemcachedKeyCharacterError(
                    "Control characters not allowed")

    if poll is not None:
        # Use poll, it's better than select
        def _send_multi(self, buffers, mark_dead = True):
            '''
            Takes a mapping server->buffer, sends the data in the buffers
            to all servers in parallel, returning unsent buffers at the end 
            (ie: due to errors)
            '''
            if not buffers:
                return {}
            sockets = {
                server.socket : [server, buf]
                for server, buf in buffers.iteritems()
            }
            fdmap = { sock.fileno() : sock for sock in sockets }.__getitem__
            unsent = {}
            buffer_ = buffer
            len_ = len
            sendflags = socket.MSG_DONTWAIT
            socket_error_timeout = socket.timeout
            socket_error = socket.error
            POLLOUT = select.POLLOUT
            POLLERR = select.POLLERR | select.POLLNVAL | select.POLLHUP
            poller = select.poll()
            for sock in sockets:
                poller.register(sock, POLLOUT)
            socket_timeout = max([server.socket_timeout for server in buffers]) * 1000
            while sockets:
                xlist = poller.poll(socket_timeout)
                elist = [ fdmap(sock) for sock,flags in xlist if flags & POLLERR ]
                wlist = [ fdmap(sock) for sock,flags in xlist if flags & POLLOUT ]
                if elist:
                    for sock, (server, buf) in sockets.iteritems():
                        unsent[server] = buf
                        if mark_dead:
                            server.mark_dead("connection reset by peer")
                        sockets.pop(sock)
                        poller.unregister(sock)
                elif not wlist:
                    # No error, no ready socket, means timeout
                    for sock, (server, buf) in sockets.iteritems():
                        unsent[server] = buf
                        if mark_dead:
                            server.mark_dead("timeout")
                    break
                for sock in wlist:
                    state = sockets[sock]
                    server, buf = state
                    try:
                        sent = sock.send(buf, sendflags)
                    except socket_error_timeout:
                        continue
                    except socket_error, msg:
                        if mark_dead:
                            if isinstance(msg, tuple):
                                msg = msg[1]
                            server.mark_dead(msg)
                        sockets.pop(sock)
                        poller.unregister(sock)
                        unsent[server] = buf
                    else:
                        if sent == len_(buf):
                            sockets.pop(sock)
                            poller.unregister(sock)
                        else:
                            state[1] = buffer_(buf, sent)
            return unsent

        def get_multi(self, keys, key_prefix=''):
            '''
            Retrieves multiple keys from the memcache doing just one query.
    
            >>> success = mc.set("foo", "bar")
            >>> success = mc.set("baz", 42)
            >>> mc.get_multi(["foo", "baz", "foobar"]) == {"foo": "bar", "baz": 42}
            1
            >>> mc.set_multi({'k1' : 1, 'k2' : 2}, key_prefix='pfx_') == []
            1
    
            This looks up keys 'pfx_k1', 'pfx_k2', ... . Returned dict will just have unprefixed keys 'k1', 'k2'.
            >>> mc.get_multi(['k1', 'k2', 'nonexist'], key_prefix='pfx_') == {'k1' : 1, 'k2' : 2}
            1
    
            get_mult [ and L{set_multi} ] can take str()-ables like ints / longs as keys too. Such as your db pri key fields.
            They're rotored through str() before being passed off to memcache, with or without the use of a key_prefix.
            In this mode, the key_prefix could be a table name, and the key itself a db primary key number.
    
            >>> mc.set_multi({42: 'douglass adams', 46 : 'and 2 just ahead of me'}, key_prefix='numkeys_') == []
            1
            >>> mc.get_multi([46, 42], key_prefix='numkeys_') == {42: 'douglass adams', 46 : 'and 2 just ahead of me'}
            1
    
            This method is recommended over regular L{get} as it lowers the number of
            total packets flying around your network, reducing total latency, since
            your app doesn't have to wait for each round-trip of L{get} before sending
            the next one.
    
            See also L{set_multi}.
    
            @param keys: An array of keys.
            @param key_prefix: A string to prefix each key when we communicate with memcache.
                Facilitates pseudo-namespaces within memcache. Returned dictionary keys will not have this prefix.
            @return:  A dictionary of key/value pairs that were available. If key_prefix was provided, the keys in the retured dictionary will not have it present.
    
            '''
    
            self._statlog('get_multi')
    
            server_keys, prefixed_to_orig_key = self._map_and_prefix_keys(keys, key_prefix)
    
            # send out all requests on each server before reading anything
            unsent = self._send_multi({
                server : "get %s\r\n" % (" ".join(server_keys[server]),)
                for server in server_keys.iterkeys()
            })
            dead_servers = unsent.keys()
            del unsent
    
            # if any servers died on the way, don't expect them to respond.
            for server in dead_servers:
                del server_keys[server]
    
            # fetch values from all servers, in interleaved fashion, skipping
            # unready servers, marking dead ones. While this isn't fully
            # nonblocking, it decreases the time sockets spend blocked due
            # to full buffers, and has a better chance to parallelize bulk 
            # network I/O from value data
            retvals = {}
            sockets = {
                server.socket : server
                for server in server_keys
            }
            if sockets:
                socket_timeout = max([server.socket_timeout for server in server_keys]) * 1000
                fdmap = { sock.fileno() : sock for sock in sockets }.__getitem__
                POLLIN = select.POLLIN
                POLLERR = select.POLLERR | select.POLLNVAL | select.POLLHUP
                poller = select.poll()
                for sock in sockets:
                    poller.register(sock, POLLIN|POLLERR)
                max_blocking_buffer = 4096
                while sockets:
                    xlist = poller.poll(socket_timeout)
                    elist = [ fdmap(sock) for sock,flags in xlist if flags & POLLERR ]
                    rlist = [ fdmap(sock) for sock,flags in xlist if flags & POLLIN ]
                    if elist:
                        for sock in sockets.iteritems():
                            sockets[sock].mark_dead("connection reset by peer")
                            sockets.pop(sock)
                            poller.unregister(sock)
                    elif not rlist:
                        # No error, no ready socket, means timeout
                        for sock in sockets:
                            sockets[sock].mark_dead("timeout")
                        break
                    for sock in rlist:
                        server = sockets[sock]
                        try:
                            while 1:
                                line = server.readline()
                                if not line or line == 'END':
                                    sockets.pop(sock)
                                    poller.unregister(sock)
                                    break
                                else:
                                    rkey, flags, rlen = self._expectvalue(server, line)
                                    #  Bo Yang reports that this can sometimes be None
                                    if rkey is not None:
                                        val = self._recv_value(server, flags, rlen)
                                        retvals[prefixed_to_orig_key[rkey]] = val   # un-prefix returned key.
                                # Go on unless there's no more lines to read
                                if not (server.buffer and (len(server.buffer) > max_blocking_buffer or '\r\n' in server.buffer)):
                                    break
                        except (memcache._Error, socket.error), msg:
                            if isinstance(msg, tuple): msg = msg[1]
                            server.mark_dead(msg)
                            sockets.pop(sock)
                            poller.unregister(sock)
            return retvals
    
        def set_multi(self, mapping, time=0, key_prefix='', min_compress_len=0):
            '''
            Sets multiple keys in the memcache doing just one query.
    
            >>> notset_keys = mc.set_multi({'key1' : 'val1', 'key2' : 'val2'})
            >>> mc.get_multi(['key1', 'key2']) == {'key1' : 'val1', 'key2' : 'val2'}
            1
    
    
            This method is recommended over regular L{set} as it lowers the number of
            total packets flying around your network, reducing total latency, since
            your app doesn't have to wait for each round-trip of L{set} before sending
            the next one.
    
            @param mapping: A dict of key/value pairs to set.
            @param time: Tells memcached the time which this value should expire, either
            as a delta number of seconds, or an absolute unix time-since-the-epoch
            value. See the memcached protocol docs section "Storage Commands"
            for more info on <exptime>. We default to 0 == cache forever.
            @param key_prefix:  Optional string to prepend to each key when sending to memcache. Allows you to efficiently stuff these keys into a pseudo-namespace in memcache:
                >>> notset_keys = mc.set_multi({'key1' : 'val1', 'key2' : 'val2'}, key_prefix='subspace_')
                >>> len(notset_keys) == 0
                True
                >>> mc.get_multi(['subspace_key1', 'subspace_key2']) == {'subspace_key1' : 'val1', 'subspace_key2' : 'val2'}
                True
    
                Causes key 'subspace_key1' and 'subspace_key2' to be set. Useful in conjunction with a higher-level layer which applies namespaces to data in memcache.
                In this case, the return result would be the list of notset original keys, prefix not applied.
    
            @param min_compress_len: The threshold length to kick in auto-compression
            of the value using the zlib.compress() routine. If the value being cached is
            a string, then the length of the string is measured, else if the value is an
            object, then the length of the pickle result is measured. If the resulting
            attempt at compression yeilds a larger string than the input, then it is
            discarded. For backwards compatability, this parameter defaults to 0,
            indicating don't ever try to compress.
            @return: List of keys which failed to be stored [ memcache out of memory, etc. ].
            @rtype: list
    
            '''
    
            self._statlog('set_multi')
    
            server_keys, prefixed_to_orig_key = self._map_and_prefix_keys(mapping.iterkeys(), key_prefix)
    
            # send out all requests on each server before reading anything
            notstored = [] # original keys.
    
            server_commands = {}
            for server in server_keys.iterkeys():
                bigcmd = []
                write = bigcmd.append
                for key in server_keys[server]: # These are mangled keys
                    store_info = self._val_to_store_info(
                            mapping[prefixed_to_orig_key[key]],
                            min_compress_len)
                    if store_info:
                        write("set %s %d %d %d\r\n%s\r\n" % (key, store_info[0],
                                time, store_info[1], store_info[2]))
                    else:
                        notstored.append(prefixed_to_orig_key[key])
                server_commands[server] = ''.join(bigcmd)
            unsent = self._send_multi(server_commands)
            dead_servers = unsent.keys()
            del unsent, server_commands
    
            # if any servers died on the way, don't expect them to respond.
            for server in dead_servers:
                del server_keys[server]
    
            #  short-circuit if there are no servers, just return all keys
            if not server_keys: return(mapping.keys())
    
            # wait for all confirmations
            sockets = {
                server.socket : [server, len(keys)]
                for server, keys in server_keys.iteritems()
            }
            if sockets:
                socket_timeout = max([server.socket_timeout for server in server_keys]) * 1000
                fdmap = { sock.fileno() : sock for sock in sockets }.__getitem__
                POLLIN = select.POLLIN
                POLLERR = select.POLLERR | select.POLLNVAL | select.POLLHUP
                poller = select.poll()
                for sock in sockets:
                    poller.register(sock, POLLIN|POLLERR)
                max_blocking_buffer = 4096
                while sockets:
                    xlist = poller.poll(socket_timeout)
                    elist = [ fdmap(sock) for sock,flags in xlist if flags & POLLERR ]
                    rlist = [ fdmap(sock) for sock,flags in xlist if flags & POLLIN ]
                    if elist:
                        for sock in sockets.iteritems():
                            server, pending_keys = sockets[sock]
                            server.mark_dead("connection reset by peer")
                            sockets.pop(sock)
                            poller.unregister(sock)
                            notstored.extend(map(prefixed_to_orig_key.__getitem__, server_keys[server][-pending_keys:]))
                    elif not rlist:
                        # No error, no ready socket, means timeout
                        for sock in sockets:
                            sockets[sock][0].mark_dead("timeout")
                        for sock, (server, pending_keys) in sockets.iteritems():
                            notstored.extend(map(prefixed_to_orig_key.__getitem__, server_keys[server][-pending_keys:]))
                        break
                    for sock in rlist:
                        state = sockets[sock]
                        server, pending_keys = state
                        try:
                            while 1:
                                line = server.readline()
                                if line == 'STORED':
                                    state[1] -= 1
                                    if state[1] <= 0:
                                        sockets.pop(sock)
                                        poller.unregister(sock)
                                        break
                                # Go on unless there's no more lines to read
                                if not (server.buffer and (len(server.buffer) > max_blocking_buffer or '\r\n' in server.buffer)):
                                    break
                        except (memcache._Error, socket.error), msg:
                            if isinstance(msg, tuple): msg = msg[1]
                            server.mark_dead(msg)
                            pending_keys = state[1]
                            notstored.extend(map(prefixed_to_orig_key.__getitem__, server_keys[server][-pending_keys:]))
                            sockets.pop(sock)
                            poller.unregister(sock)
            return notstored

    else:
        # fall back to select
        def _send_multi(self, buffers, mark_dead = True):  # lint:ok
            '''
            Takes a mapping server->buffer, sends the data in the buffers
            to all servers in parallel, returning unsent buffers at the end 
            (ie: due to errors)
            '''
            if not buffers:
                return {}
            sockets = {
                server.socket : [server, buf]
                for server, buf in buffers.iteritems()
            }
            unsent = {}
            pops = []
            buffer_ = buffer
            len_ = len
            sendflags = socket.MSG_DONTWAIT
            socket_error_timeout = socket.timeout
            socket_error = socket.error
            select_ = select.select
            socket_timeout = max([server.socket_timeout for server in buffers])
            while sockets:
                rlist, wlist, xlist = select_((), sockets.keys(), (), socket_timeout)
                if not wlist:
                    for sock, (server, buf) in sockets.iteritems():
                        unsent[server] = buf
                    return
                for sock in wlist:
                    state = sockets[sock]
                    server, buf = state
                    try:
                        sent = sock.send(buf, sendflags)
                    except socket_error_timeout:
                        continue
                    except socket_error, msg:
                        if mark_dead:
                            if isinstance(msg, tuple):
                                msg = msg[1]
                            server.mark_dead(msg)
                        pops.append(sock)
                    if sent == len_(buf):
                        state[1] = None
                        pops.append(sock)
                    else:
                        state[1] = buffer_(buf, sent)
                for sock in pops:
                    server, buf = sockets.pop(sock)
                    if buf is not None:
                        unsent[server] = buf
                del pops[:]
            return unsent

        def get_multi(self, keys, key_prefix=''):  # lint:ok
            '''
            Retrieves multiple keys from the memcache doing just one query.
    
            >>> success = mc.set("foo", "bar")
            >>> success = mc.set("baz", 42)
            >>> mc.get_multi(["foo", "baz", "foobar"]) == {"foo": "bar", "baz": 42}
            1
            >>> mc.set_multi({'k1' : 1, 'k2' : 2}, key_prefix='pfx_') == []
            1
    
            This looks up keys 'pfx_k1', 'pfx_k2', ... . Returned dict will just have unprefixed keys 'k1', 'k2'.
            >>> mc.get_multi(['k1', 'k2', 'nonexist'], key_prefix='pfx_') == {'k1' : 1, 'k2' : 2}
            1
    
            get_mult [ and L{set_multi} ] can take str()-ables like ints / longs as keys too. Such as your db pri key fields.
            They're rotored through str() before being passed off to memcache, with or without the use of a key_prefix.
            In this mode, the key_prefix could be a table name, and the key itself a db primary key number.
    
            >>> mc.set_multi({42: 'douglass adams', 46 : 'and 2 just ahead of me'}, key_prefix='numkeys_') == []
            1
            >>> mc.get_multi([46, 42], key_prefix='numkeys_') == {42: 'douglass adams', 46 : 'and 2 just ahead of me'}
            1
    
            This method is recommended over regular L{get} as it lowers the number of
            total packets flying around your network, reducing total latency, since
            your app doesn't have to wait for each round-trip of L{get} before sending
            the next one.
    
            See also L{set_multi}.
    
            @param keys: An array of keys.
            @param key_prefix: A string to prefix each key when we communicate with memcache.
                Facilitates pseudo-namespaces within memcache. Returned dictionary keys will not have this prefix.
            @return:  A dictionary of key/value pairs that were available. If key_prefix was provided, the keys in the retured dictionary will not have it present.
    
            '''
    
            self._statlog('get_multi')
    
            server_keys, prefixed_to_orig_key = self._map_and_prefix_keys(keys, key_prefix)
    
            # send out all requests on each server before reading anything
            unsent = self._send_multi({
                server : "get %s\r\n" % (" ".join(server_keys[server]),)
                for server in server_keys.iterkeys()
            })
            dead_servers = unsent.keys()
            del unsent
    
            # if any servers died on the way, don't expect them to respond.
            for server in dead_servers:
                del server_keys[server]
    
            # fetch values from all servers, in interleaved fashion, skipping
            # unready servers, marking dead ones. While this isn't fully
            # nonblocking, it decreases the time sockets spend blocked due
            # to full buffers, and has a better chance to parallelize bulk 
            # network I/O from value data
            retvals = {}
            sockets = {
                server.socket : server
                for server in server_keys
            }
            if sockets:
                socket_timeout = max([server.socket_timeout for server in server_keys])
                select_ = select.select
                max_blocking_buffer = 4096
                while sockets:
                    rlist, wlist, xlist = select_(sockets.keys(), (), (), socket_timeout)
                    if not rlist:
                        for sock in sockets:
                            sockets[sock].mark_dead("timeout")
                        break
                    for sock in rlist:
                        server = sockets[sock]
                        try:
                            while 1:
                                line = server.readline()
                                if not line or line == 'END':
                                    sockets.pop(sock)
                                    break
                                else:
                                    rkey, flags, rlen = self._expectvalue(server, line)
                                    #  Bo Yang reports that this can sometimes be None
                                    if rkey is not None:
                                        val = self._recv_value(server, flags, rlen)
                                        retvals[prefixed_to_orig_key[rkey]] = val   # un-prefix returned key.
                                # Go on unless there's no more lines to read
                                if not (server.buffer and (len(server.buffer) > max_blocking_buffer or '\r\n' in server.buffer)):
                                    break
                        except (memcache._Error, socket.error), msg:
                            if isinstance(msg, tuple): msg = msg[1]
                            server.mark_dead(msg)
                            sockets.pop(sock)
            return retvals
    
        def set_multi(self, mapping, time=0, key_prefix='', min_compress_len=0):  # lint:ok
            '''
            Sets multiple keys in the memcache doing just one query.
    
            >>> notset_keys = mc.set_multi({'key1' : 'val1', 'key2' : 'val2'})
            >>> mc.get_multi(['key1', 'key2']) == {'key1' : 'val1', 'key2' : 'val2'}
            1
    
    
            This method is recommended over regular L{set} as it lowers the number of
            total packets flying around your network, reducing total latency, since
            your app doesn't have to wait for each round-trip of L{set} before sending
            the next one.
    
            @param mapping: A dict of key/value pairs to set.
            @param time: Tells memcached the time which this value should expire, either
            as a delta number of seconds, or an absolute unix time-since-the-epoch
            value. See the memcached protocol docs section "Storage Commands"
            for more info on <exptime>. We default to 0 == cache forever.
            @param key_prefix:  Optional string to prepend to each key when sending to memcache. Allows you to efficiently stuff these keys into a pseudo-namespace in memcache:
                >>> notset_keys = mc.set_multi({'key1' : 'val1', 'key2' : 'val2'}, key_prefix='subspace_')
                >>> len(notset_keys) == 0
                True
                >>> mc.get_multi(['subspace_key1', 'subspace_key2']) == {'subspace_key1' : 'val1', 'subspace_key2' : 'val2'}
                True
    
                Causes key 'subspace_key1' and 'subspace_key2' to be set. Useful in conjunction with a higher-level layer which applies namespaces to data in memcache.
                In this case, the return result would be the list of notset original keys, prefix not applied.
    
            @param min_compress_len: The threshold length to kick in auto-compression
            of the value using the zlib.compress() routine. If the value being cached is
            a string, then the length of the string is measured, else if the value is an
            object, then the length of the pickle result is measured. If the resulting
            attempt at compression yeilds a larger string than the input, then it is
            discarded. For backwards compatability, this parameter defaults to 0,
            indicating don't ever try to compress.
            @return: List of keys which failed to be stored [ memcache out of memory, etc. ].
            @rtype: list
    
            '''
    
            self._statlog('set_multi')
    
            server_keys, prefixed_to_orig_key = self._map_and_prefix_keys(mapping.iterkeys(), key_prefix)
    
            # send out all requests on each server before reading anything
            notstored = [] # original keys.
    
            server_commands = {}
            for server in server_keys.iterkeys():
                bigcmd = []
                write = bigcmd.append
                for key in server_keys[server]: # These are mangled keys
                    store_info = self._val_to_store_info(
                            mapping[prefixed_to_orig_key[key]],
                            min_compress_len)
                    if store_info:
                        write("set %s %d %d %d\r\n%s\r\n" % (key, store_info[0],
                                time, store_info[1], store_info[2]))
                    else:
                        notstored.append(prefixed_to_orig_key[key])
                server_commands[server] = ''.join(bigcmd)
            unsent = self._send_multi(server_commands)
            dead_servers = unsent.keys()
            del unsent, server_commands
    
            # if any servers died on the way, don't expect them to respond.
            for server in dead_servers:
                del server_keys[server]
    
            #  short-circuit if there are no servers, just return all keys
            if not server_keys: return(mapping.keys())
    
            # wait for all confirmations
            sockets = {
                server.socket : [server, len(keys)]
                for server, keys in server_keys.iteritems()
            }
            if sockets:
                socket_timeout = max([server.socket_timeout for server in server_keys])
                select_ = select.select
                max_blocking_buffer = 4096
                while sockets:
                    rlist, wlist, xlist = select_(sockets.keys(), (), (), socket_timeout)
                    if not rlist:
                        for sock in sockets:
                            sockets[sock][0].mark_dead("timeout")
                        for sock, (server, pending_keys) in sockets.iteritems():
                            notstored.extend(map(prefixed_to_orig_key.__getitem__, server_keys[server][-pending_keys:]))
                        break
                    for sock in rlist:
                        state = sockets[sock]
                        server, pending_keys = state
                        try:
                            while 1:
                                line = server.readline()
                                if line == 'STORED':
                                    state[1] -= 1
                                    if state[1] <= 0:
                                        sockets.pop(sock)
                                        break
                                # Go on unless there's no more lines to read
                                if not (server.buffer and (len(server.buffer) > max_blocking_buffer or '\r\n' in server.buffer)):
                                    break
                        except (memcache._Error, socket.error), msg:
                            if isinstance(msg, tuple): msg = msg[1]
                            server.mark_dead(msg)
                            pending_keys = state[1]
                            notstored.extend(map(prefixed_to_orig_key.__getitem__, server_keys[server][-pending_keys:]))
                            sockets.pop(sock)
            return notstored


class DynamicResolvingMemcachedClient(BaseCacheClient, ThreadLocalDynamicResolvingClient):
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
            key_pickler = None,
            client_pickler = None,
            client_unpickler = None,
            client_pickler_key = ';',
            namespace = None,
            compress = True,
            compress_prefix = default_compression_pfx,
            compress_file_class = default_compress_file_class,
            decompress_fn = default_decompress_fn,
            checksum_key = None, # CHANGE IT!
            encoding_cache = None, # should be able to contain attributes
            client_class = MemcachedStoreClient,
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
        self.namespace = namespace
        self.failfast_time = failfast_time
        self.succeedfast_time = succeedfast_time
        self.encoding_cache = encoding_cache
        self.compress = compress
        self.compress_prefix = compress_prefix
        self.compress_file_class = compress_file_class
        self.decompress_fn = decompress_fn

        if client_pickler is None:
            self.client_pickler = lambda *p, **kw: sPickle.SecurePickler(checksum_key, *p, **kw)
            self.client_unpickler = lambda *p, **kw: sPickle.SecureUnpickler(checksum_key, *p, **kw)
            self.client_pickler_key = '%s,' % (sPickle.checksum_algo_name,)
        else:
            self.client_pickler = client_pickler
            self.client_unpickler = client_unpickler
            self.client_pickler_key = client_pickler_key

        self.pickler = pickler or cPickle
        self.key_pickler = key_pickler or self.pickler
        
        if self.namespace:
            self.max_backing_key_length -= len(self.namespace)+1
        
        assert self.max_backing_key_length > 48
        assert self.max_backing_value_length > 128
        
        if 'pickleProtocol' not in client_args:
            # use binary protocol, otherwise binary data gets inflated
            # unreasonably when pickling
            client_args['pickleProtocol'] = 2

        client_args['pickler'] = self.client_pickler
        client_args['unpickler'] = self.client_unpickler

        self._failfast_cache = Cache(failfast_size)
        self._succeedfast_cache = Cache(succeedfast_size)
        self._usucceedfast_cache = Cache(succeedfast_size)

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
        if not isinstance(key, basestring):
            try:
                # Try JSON
                key = "J#"+json.dumps(key, separators=JSON_SEPARATORS)
                zpfx = self.compress_prefix
            except:
                # Try pickling
                key = "P#"+self.key_pickler.dumps(key,2).encode("base64").replace("\n","")
                zpfx = self.compress_prefix
        elif isinstance(key, unicode):
            key = "U#" + key.encode("utf-8")
            zpfx = self.compress_prefix
        else:
            zpfx = self.compress_prefix + '#'

        # keys cannot contain control characters or spaces
        if any(imap(ord, key.translate(tmap))):
            key = "B#" + key.encode("base64").replace("\n","")
            zpfx = self.compress_prefix

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
        
        return "%s%s" % (self.client_pickler_key, key), exact
    
    def get_version_stamp(self, short_key = None):
        if short_key is None:
            stamp_key = stamp_prefix
        else:
            # 8-bit hash to spread stamp keys, doesn't really need to be totally uniform, just almost
            stamp_key = "%s%02x" % (stamp_prefix, abs(zlib.adler32(short_key)) % 127)
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
    
    def encode_pages(self, short_key, key, ttl, value):
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
                with self.compress_file_class(sio, 1) as zio:
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
        if npages > 1:
            version = self.get_version_stamp(short_key)
        else:
            # No need to do versioning for single-page values
            version = 1
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
            value = self.decompress_fn(data)
        else:
            value = data
        value = self.pickler.loads(value)

        if self.encoding_cache is not None and isinstance(value, tuple) and len(value) > 1:
            self.encoding_cache.cache = (value[1], data)
        
        return value
    
    def _getTtl(self, key, default, decode = True, ttl_skip = None, promote_callback = None, 
            short_key = None, pages = None, method = None, multi_method = None,
            force_all_pages = False, valid_sequence_types = (list, tuple) ):
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
            _method = self.client.get
            multi_method = self.client.get_multi
            _usucceedfast_cache = self._usucceedfast_cache
            _succeedfast_time = self.succeedfast_time
            def method(k):
                cached = _usucceedfast_cache.get(k, NONE)
                if cached is not NONE:
                    cached, cached_time = cached
                    if cached_time > (now - _succeedfast_time):
                        return cached
                cached = _method(k)
                _usucceedfast_cache[k] = (cached, now)
                return cached

        if pages is None:
            pages = { 0 : method(short_key+"|0") }
        if pages[0] is None or not isinstance(pages[0],valid_sequence_types) or len(pages[0]) != 5:
            return default, -1
        
        ttl = pages[0][2]
        npages = pages[0][0]

        if not decode:
            if force_all_pages and npages > 1:
                if npages > 2 and multi_method is not None:
                    pages.update( multi_method(xrange(1,npages), key_prefix=short_key+"|") )
                else:
                    pages.update([ (i,method("%s|%d" % (short_key,i))) for i in xrange(1,npages) ])
            return pages, ttl - now
        elif ttl_skip is not None and ttl < ttl_skip:
            return default, -1
        # Check failfast cache, before making a huge effort decoding for not
        # When there's a key collision, this avoids misses being expensive
        elif self._failfast_cache.get(key) > (now - self.failfast_time):
            return default, -1
        
        if npages > 1:
            if npages > 2 and multi_method:
                pages.update( multi_method(xrange(1,npages), key_prefix=short_key+"|") )
            else:
                pages.update([ (i,method("%s|%d" % (short_key,i))) for i in xrange(1,npages) ])
        
        try:
            try:
                cached_key, cached_value = self.decode_pages(pages, key)
            except ValueError, e:
                if npages > 1 and multi_method and e.message == "Inconsistent data in cache":
                    # try again, maybe there was a write between gets
                    pages.update( multi_method(xrange(npages), key_prefix=short_key+"|") )
                    cached_key, cached_value = self.decode_pages(pages, key)
                else:
                    # unrecoverable
                    raise
            
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

    def renew(self, key, ttl):
        old_cas = getattr(self.client, 'cache_cas', None)
        reset_cas = old_cas
        if old_cas is not None and not old_cas:
            self.client.cache_cas = reset_cas = True
        try:
            short_key,exact = self.shorten_key(key)
            raw_pages, store_ttl = self._getTtl(key, NONE, False, short_key = short_key, 
                method = self.client.gets, force_all_pages = True)
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
                    else:
                        # No point in going on
                        break
        finally:
            if reset_cas:
                self.client.cache_cas = old_cas
                self.client.reset_cas()
    
    def put(self, key, value, ttl):
        # set_multi all pages in one roundtrip
        short_key,exact = self.shorten_key(key)
        pages = dict([(page,data) for page,data in enumerate(self.encode_pages(
            short_key, key, ttl+time.time(), value))])
        self.client.set_multi(pages, min(ttl, MAX_MEMCACHE_TTL), key_prefix=short_key+"|")
        
        try:
            del self._failfast_cache[key]
        except:
            pass
        
        try:
            del self._succeedfast_cache[key]
        except:
            pass

        for page in pages:
            try:
                del self._usucceedfast_cache[short_key+"|%s" % (page,)]
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
        else:
            npages = 1
        
        try:
            del self._succeedfast_cache[key]
        except:
            pass

        for page in xrange(npages):
            try:
                del self._usucceedfast_cache[short_key+"|%d" % (page,)]
            except:
                pass
    
    def clear(self):
        # We don't want to clear memcache, it might be shared
        self._failfast_cache.clear()
        self._succeedfast_cache.clear()
        self._usucceedfast_cache.clear()

    def purge(self):
        # Memcache does that itself
        self._failfast_cache.clear()
        self._succeedfast_cache.clear()
        self._usucceedfast_cache.clear()
    
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
            client_class = MemcachedStoreClient,
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
            encode = self.encode
            encode_key = self.encode_key
            for i in xrange(2):
                # It can explode if a thread lingers, so restart if that happens
                try:
                    for key, (value, ttl) in workset.iteritems():
                        key = encode_key(key)
                        if value is NONE:
                            deletions.append(key)
                        elif value is _RENEW:
                            renewals.append((key, ttl))
                        else:
                            plan[ttl][key] = encode(key, ttl+quicknow, value)
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
                client = self.client
                if deletions:
                    try:
                        client.delete_multi(deletions)
                    except:
                        logging.error("Exception in background writer", exc_info = True)
                if plan:
                    for ttl, batch in plan.iteritems():
                        try:
                            ttl = min(ttl, MAX_MEMCACHE_TTL)
                            client.set_multi(batch, ttl)
                        except:
                            logging.error("Exception in background writer", exc_info = True)
                if renewals:
                    for key, ttl in renewals:
                        value = client.gets(key)
                        if value is not None:
                            value, kttl = self.decode(value)
                            nttl = ttl + quicknow
                            if kttl < nttl:
                                value = encode(key, nttl, value)
                                ttl = min(ttl, MAX_MEMCACHE_TTL)
                                client.cas(key, value, ttl)
                
                # Let us be suicidal
                del client
                workset.clear()

            del self, plan, deletions, renewals, encode, encode_key
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
    
    def _getTtl(self, key, default, ttl_skip = None, promote_callback = None, encoded = False, raw_key = None):
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

    def getTtl(self, key, default = NONE, ttl_skip = None, **kw):
        # This trampoline is necessary to avoid re-entrancy issues when this client
        # is wrapped inside a SyncWrapper. Internal calls go directly to _getTtl
        # to avoid locking the wrapper's mutex.
        return self._getTtl(key, default, ttl_skip = ttl_skip, **kw)
    
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
