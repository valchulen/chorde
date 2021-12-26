# -*- coding: utf-8 -*-
from threading import RLock, Lock, Condition
import time
import logging
import sys

from weakref import WeakValueDictionary
from functools import wraps

__all__ = [
    "Lock",
    "RLock",
    "Condition",
    "ReadWriteLock",
    "ReadWriteRLock",
]


class ReadWriteLock(object):
    __slots__ = [ '_read_ready', '_readers' ]

    locktype = staticmethod(Lock)

    def __init__(self):
        self._read_ready = Condition(self.locktype())
        self._readers = 0

    def acquire_read(self, blocking=1):
        if not self._read_ready.acquire(blocking):
            return False
        try:
            self._readers += 1
            return True
        finally:
            self._read_ready.release()

    def release_read(self):
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notifyAll()
        finally:
            self._read_ready.release()

    def acquire_write(self, blocking=1):
        if not self._read_ready.acquire(blocking):
            return False
        while self._readers > 0:
            self._read_ready.wait(blocking or None)
            if not blocking and self._readers > 0:
                self._read_ready.release()
                return False
        return True

    def release_write(self):
        self._read_ready.release()

class ReadWriteRLock(ReadWriteLock):
    __slots__ = []
    locktype = staticmethod(RLock)

def serialize_docstring(f):
    if 'sync' not in (getattr(f, '__doc__', '') or '').lower():
        f.__doc__ = 'Synchronized\n\n' + (getattr(f, '__doc__', '') or '')
    return f

_initialization_lock = Lock()

def serialize(*p,**kw):
    if 'pydoc' in sys.argv[0]:
        if len(p) == 1:
            return serialize_docstring(*p)
        else:
            return serialize_docstring

    acquire   = kw.get('acquire','acquire')
    release   = kw.get('release','release')

    if 'lockobj' in kw:
        # pre-assigned lock object, unkeyed
        lockobj = kw['lockobj']
        def decor(f):
            @wraps(f)
            def rv(*pos,**kw):
                try:
                    getattr(lockobj, acquire)()
                    return f(*pos,**kw)
                finally:
                    getattr(lockobj, release)()
            return rv
    else:
        # unkeyed lock attribute
        locktype = kw.get('locktype',RLock)
        lockattr = kw.get('lockmember','__serialization_lock')
        def decor(f):  # lint:ok
            @wraps(f)
            def rv(self,*pos,**kw):
                if not hasattr(self,lockattr):
                    with _initialization_lock:
                        if not hasattr(self,lockattr):
                            setattr(self,lockattr, locktype())
                lock = getattr(self,lockattr)
                try:
                    getattr(lock, acquire)()
                    return f(self,*pos,**kw)
                finally:
                    getattr(lock, release)()
            return rv

    if len(p) == 1:
        return decor(p[0])
    else:
        return decor

def serialize_read(*p,**kw):
    kw['locktype']= kw.get('locktype', ReadWriteRLock)
    kw['acquire'] = kw.get('acquire', 'acquire_read')
    kw['release'] = kw.get('release', 'release_read')
    return serialize(*p,**kw)

def serialize_write(*p,**kw):
    kw['locktype']= kw.get('locktype', ReadWriteRLock)
    kw['acquire'] = kw.get('acquire', 'acquire_write')
    kw['release'] = kw.get('release', 'release_write')
    return serialize(*p,**kw)
