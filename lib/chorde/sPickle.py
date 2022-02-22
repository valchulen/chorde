# -*- coding: utf-8 -*-
# This monster makes it all compatible with up to py2.0 :-o

__all__ = [
    "SecurePickler",
    "SecureUnpickler",
]

#lint:disable
try:
    from hashlib import sha256 as checksum_algo
except ImportError:
    try:
        from hashlib import sha1 as checksum_algo
    except ImportError:
        try:
            from hashlib import md5 as checksum_algo
        except ImportError:
            try:
                from sha import sha as checksum_algo
            except ImportError:
                from md5 import md5 as checksum_algo
#lint:enable
checksum_algo_name = checksum_algo.__name__.replace('openssl_','')

import hmac
import struct
import threading
from binascii import hexlify, unhexlify

import pickle as cPickle  # lint:ok
from io import BytesIO  # lint:ok

class SecurePickler(object):
    def __init__(self, checksum_key, file, *p, **kw):
        self.file = file
        self.checksum_key = checksum_key

        self.backing_class = kw.pop('backing_class', cPickle.Pickler)
        self.backing_args = (p, kw)
        self.local = threading.local()

    @property
    def buf(self):
        try:
            return self.local.buf
        except AttributeError:
            self.local.buf = buf = BytesIO()
            return buf

    @buf.deleter
    def buf(self):  # lint:ok
        del self.local.buf

    @property
    def pickler(self):
        try:
            return self.local.pickler
        except AttributeError:
            p, kw = self.backing_args
            self.local.pickler = pickler = self.backing_class(self.buf, *p, **kw)
            return pickler

    @pickler.deleter
    def pickler(self):  # lint:ok
        del self.local.pickler

    @property
    def persistent_id(self):
        return self.pickler.persistent_id

    @persistent_id.setter
    def persistent_id(self, value):  # lint:ok
        self.pickler.persistent_id = value

    def dump(self,val):
        # dump to underlying pickler, then pick up the results
        self.pickler.dump(val)
        rv = self.buf.getvalue()
        self.buf.seek(0)
        self.buf.truncate()

        # compute HMAC, and prepend to output
        md = hmac.HMAC(self.checksum_key, rv, checksum_algo).hexdigest().encode("ascii")
        self.file.write(hexlify(struct.pack('<L',len(rv))))
        self.file.write(md)
        self.file.write(rv)

class SecureUnpickler(object):
    def __init__(self, checksum_key, file, *p, **kw):
        self.file = file
        self.checksum_key = checksum_key

        self.backing_class = kw.pop('backing_class', cPickle.Unpickler)
        self.backing_args = (p, kw)
        self.local = threading.local()

    @property
    def buf(self):
        try:
            return self.local.buf
        except AttributeError:
            self.local.buf = buf = BytesIO()
            return buf

    @buf.deleter
    def buf(self):  # lint:ok
        del self.local.buf

    @property
    def unpickler(self):
        try:
            return self.local.unpickler
        except AttributeError:
            p, kw = self.backing_args
            self.local.unpickler = unpickler = self.backing_class(self.buf, *p, **kw)
            return unpickler

    @unpickler.deleter
    def unpickler(self):  # lint:ok
        del self.local.unpickler

    @property
    def persistent_load(self):
        return self.unpickler.persistent_load

    @persistent_load.setter
    def persistent_load(self, value):  # lint:ok
        self.unpickler.persistent_load = value

    def load(self, headlen = len(struct.pack('<L',0)) * 2):
        datalen = self.file.read(headlen)
        if not datalen:
            raise EOFError("Cannot read secure packet header")
        datalen, = struct.unpack('<L', unhexlify(datalen) )

        ref_md = hmac.HMAC(self.checksum_key, None, checksum_algo)
        md = self.file.read(ref_md.digest_size*2)

        data = self.file.read(datalen)
        ref_md.update(data)

        ref_md = ref_md.hexdigest().encode("ascii")
        if ref_md != md:
            raise ValueError("MAC mismatch unpickling")

        buf = self.buf
        buf.seek(0)
        buf.write(data)
        buf.truncate()
        buf.seek(0)
        rv = self.unpickler.load()
        buf.seek(0)
        buf.truncate()
        return rv

def dump(key, obj, file, *p, **kw):
    pickler = SecurePickler(key, file, *p, **kw)
    pickler.dump(obj)

def dumps(key, obj, *p, **kw):
    buf = BytesIO()
    pickler = SecurePickler(key, buf, *p, **kw)
    pickler.dump(obj)
    return buf.getvalue()

def load(key, file, *p, **kw):
    unpickler = SecureUnpickler(key, file, *p, **kw)
    return unpickler.load()

def loads(key, str, *p, **kw):
    buf = BytesIO(str)
    unpickler = SecureUnpickler(key, buf, *p, **kw)
    return unpickler.load()
