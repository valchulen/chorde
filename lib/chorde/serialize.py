# -*- coding: utf-8 -*-
from threading import RLock, Lock, Condition
import time
import logging
import sys

from weakref import WeakValueDictionary
from functools import wraps

__all__ = [
    "ScopedLock",

    "Lock",
    "RLock",
    "Condition",
    "LockPool",
    "RLockPool",
    "ReadWriteLock",
    "ReadWriteRLock",

    "DeadlockError",
    "LockWouldBlock",
]

class ScopedLock(object):
    __slots__ = ['mutex','__weakref__']

    def __init__(self,mutex,blocking=1):
        self.mutex = mutex
        if not self.mutex.acquire(blocking):
            raise LockWouldBlock

    def __del__(self):
        self.mutex.release()

class WGLock(object):
    __slots__ = ['_WGLock__lock', '__weakref__']

    """ Generic weakly referenceable lock - supply a 'locktype' class attribute to specify lock type """
    def __init__(self):
        self.__lock = self.locktype() # False positive: UndefinedAttributeAccessWarning  (it's an abstract class that expects this attribute to be overriden)
    def acquire(self, blocking=1):
        return self.__lock.acquire(blocking)
    def release(self):
        return self.__lock.release()

class WLock(WGLock):
    __slots__ = []
    locktype = staticmethod(Lock)

class WRLock(WGLock):
    __slots__ = []
    locktype = staticmethod(RLock)

class GWLockPool(object):
    __slots__ = ['_GWLockPool__idLocks', '_GWLockPool__lock']

    """ Generic lock pool - supply a 'locktype' class attribute to specify lock type (must be weakly referenceable) """
    def __init__(self):
        self.__idLocks = WeakValueDictionary()
        self.__lock = Lock()

    def __get(self, id):
        try:
            self.__lock.acquire()
            try:
                lock = self.__idLocks[id]
            except KeyError:
                lock = self.locktype() # False positive: UndefinedAttributeAccessWarning  (it's an abstract class that expects this attribute to be overriden)
                self.__idLocks[id] = lock
            return lock
        finally:
            self.__lock.release()

    def acquire(self, id, blocking=1):
        try:
            return ScopedLock( self.__get(id), blocking )
        except LockWouldBlock:
            return None

    def __len__(self):
        return len(self.__idLocks)

class WLockPool(GWLockPool):
    __slots__ = []
    locktype = staticmethod(WLock)

class RWLockPool(GWLockPool):
    __slots__ = []
    locktype = staticmethod(WRLock)

class GLockPool(object):
    __slots__ = ['_GLockPool__idLocks', '_GLockPool__lock']

    """ Generic lock pool - supply a 'locktype' class attribute to specify lock type (must be weakly referenceable) """
    def __init__(self):
        self.__idLocks = {}
        self.__lock = Lock()

    class _scopedLock(object):
        __slots__ = ['pool','id']

        def __init__(self, pool, id):
            self.pool = pool
            self.id = id

        def __del__(self):
            self.pool._release(self.id)

    def __get(self, id, refs):
        glock = self.__lock
        idLocks = self.__idLocks
        try:
            glock.acquire()
            try:
                lock = idLocks[id]
                lock[1] += refs
            except KeyError:
                idLocks[id] = lock = [self.locktype(),refs] # False positive: UndefinedAttributeAccessWarning  (it's an abstract class that expects this attribute to be overriden)
            return lock
        finally:
            glock.release()

    def __del(self, id):
        glock = self.__lock
        idLocks = self.__idLocks
        try:
            glock.acquire()
            try:
                lock = idLocks[id]
                if not lock[1]:
                    del idLocks[id]
            except KeyError:
                return
        finally:
            glock.release()

    def acquire(self, id, blocking=1):
        self._acquire( id, blocking )
        return self._scopedLock(self,id)

    def _acquire(self, id, blocking=1):
        lock = self.__get(id,1)
        lock[0].acquire( blocking )

    def _release(self, id):
        lock = self.__get(id,-1)
        lock[0].release()

        if not lock[1]:
            self.__del(id)

    def __len__(self):
        return len(self.__idLocks)

class LockPool(GLockPool):
    __slots__ = []
    locktype = staticmethod(Lock)

class RLockPool(GLockPool):
    __slots__ = []
    locktype = staticmethod(RLock)


class DeadlockError(Exception):
    pass

class LockWouldBlock(Exception):
    pass


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

class ReadLockView(object):
    __slots__ = [ 'backing' ]

    def __init__(self, backing):
        self.backing = backing
    def acquire(self, blocking=1):
        return self.backing.acquire_read(blocking)
    def release(self):
        return self.backing.release_read()

class WriteLockView(object):
    __slots__ = [ 'backing' ]

    def __init__(self, backing):
        self.backing = backing
    def acquire(self, blocking=1):
        return self.backing.acquire_write(blocking)
    def release(self):
        return self.backing.release_write()

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

    generator = kw.get('generator', False)
    acquire   = kw.get('acquire','acquire')
    release   = kw.get('release','release')
    keyfn     = kw.get('key',None)
    deadlock_timeout = kw.get('deadlock_timeout',None)

    if deadlock_timeout is None:
        def deadlock_watchdog(f,acquire):
            # deadlock_timeout to None means disabling the watchdog, which is
            # the default (for performance - Python's locks don't have timeouts
            # so implementing a watchdog would hurt performance).
            return acquire()
    elif deadlock_timeout == 0:
        def deadlock_watchdog(f,acquire):  # lint:ok
            # deadlock_timeout == 0 means no timeout, but do watch for deadlocks.
            # with a nice and gentle sampling period of 100ms, will only log deadlocks
            # (since we have no way of knowing for certain when a deadlock happens)
            # when we've been waiting for over 5 minutes (which is more than any
            # lock usually spends waiting).
            waiting = 0.0
            while True:
                rv = acquire(0)
                if rv:
                    return rv
                else:
                    if waiting > 300.0:
                        import traceback
                        logging.warn("Possible deadlock waiting for %s lock at:\n%s",
                            f.__name__,
                            ''.join(traceback.format_stack()[:-2]))
                        waiting = 0.0
                    time.sleep(0.1)
                    waiting += 0.1
    elif deadlock_timeout > 0:
        def deadlock_watchdog(f,acquire):  # lint:ok
            # with a nice timout specified by client code, will try to acquire
            # nonblockingly during that time, with a nice and gentle sampling period
            # of 100ms.
            # Will raise an exception if all else fails
            sleepstep = min(0.1, deadlock_timeout)
            spins = max(1,int(deadlock_timeout / sleepstep))
            for i in xrange(spins):
                rv = acquire(0)
                if rv:
                    return rv
                if (i+1) < spins:
                    time.sleep(sleepstep)
            else:
                raise DeadlockError, "Timeout expired after %s seconds waiting for %s" % (deadlock_timeout,f.__name__)
    else:
        raise ValueError, "Invalid deadlock_timeout - must be a nonnegative number"

    if 'lockobj' in kw:
        if not keyfn:
            # pre-assigned lock object, unkeyed
            lockobj = kw['lockobj']
            def decor(f):
                if generator:
                    raise NotImplementedError
                else:
                    @wraps(f)
                    def rv(*pos,**kw):
                        try:
                            rv = deadlock_watchdog(f,getattr(lockobj, acquire))  # lint:ok
                            return f(*pos,**kw)
                        finally:
                            getattr(lockobj, release)()
                return rv
        else:
            # pre-assigned lock pool, keyed
            lockpool = kw['lockobj']
            if not isinstance(lockpool,GLockPool):
                raise TypeError("lockobj must be a GLockPool when a key is specified")
            def decor(f):  # lint:ok
                if generator:
                    raise NotImplementedError
                else:
                    @wraps(f)
                    def rv(*pos,**kw):
                        key = keyfn(*pos,**kw)
                        acq = lambda blocking=1:lockpool._acquire(key,blocking)
                        rel = lambda : lockpool._release(key)
                        deadlock_watchdog(f,acq)
                        try:
                            return f(*pos,**kw)
                        finally:
                            rel()
                return rv
    elif not keyfn:
        # unkeyed lock attribute
        locktype = kw.get('locktype',RLock)
        lockattr = kw.get('lockmember','__serialization_lock')
        def decor(f):  # lint:ok
            if generator:
                raise NotImplementedError
            else:
                @wraps(f)
                def rv(self,*pos,**kw):
                    if not hasattr(self,lockattr):
                        with _initialization_lock:
                            if not hasattr(self,lockattr):
                                setattr(self,lockattr, locktype())
                    lock = getattr(self,lockattr)
                    try:
                        deadlock_watchdog(f,getattr(lock, acquire))
                        return f(self,*pos,**kw)
                    finally:
                        getattr(lock, release)()
            return rv
    else:
        # keyed lock attribute
        locktype = kw.get('locktype',RLock)
        lockpooltype = kw.get('lockpooltype',LockPool)
        def decor(f):  # lint:ok
            lockattr = kw.get('lockmember','__serialization_lock_' + f.__name__)
            if generator:
                raise NotImplementedError
            else:
                @wraps(f)
                def rv(self,*pos,**kw):
                    if not hasattr(self,lockattr):
                        with _initialization_lock:
                            if not hasattr(self,lockattr):
                                setattr(self,lockattr, lockpooltype())
                    lockpool = getattr(self,lockattr)
                    acq = lambda blocking=1:lockpool.acquire(keyfn(self,*pos,**kw),blocking)
                    lock = deadlock_watchdog(f,acq)
                    try:
                        return f(self,*pos,**kw)
                    finally:
                        del lock
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
