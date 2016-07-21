import functools
import weakref
import threading
import logging

import cython

from chorde.clients import base

cdef object CacheMissError, CancelledError, TimeoutError
cdef object CacheMissErrorCached, CancelledErrorCached, TimeoutErrorCached
cdef object wref, functools_partial
CacheMissError = base.CacheMissError
CancelledError = base.CancelledError
TimeoutError = base.TimeoutError
CacheMissErrorCached = CacheMissError()
CancelledErrorCached = CancelledError()
TimeoutErrorCached = TimeoutError()
wref = weakref.ref
functools_partial = functools.partial

@cython.freelist(100)
cdef class ExceptionWrapper:
    cdef public object value
    cdef object __weakref__

    def __cinit__(self, value):
        self.value = value

@cython.freelist(100)
cdef class WeakCallback:
    cdef object me, callback, __weakref__

    def __cinit__(self, me, callback):
        self.me = wref(me)
        self.callback = callback

    def __call__(self, value):
        cdef object me
        me = self.me()
        if me is not None:
            return self.callback(me)

@cython.freelist(100)
cdef class DeferExceptionCallback:
    cdef object defer, __weakref__
    def __cinit__(self, defer):
        self.defer = defer
    def __call__(self, value):
        self.defer.set_exception(value[1] or value[0])

@cython.freelist(100)
cdef class ValueCallback:
    cdef object callback, __weakref__
    def __cinit__(self, callback):
        self.callback = callback
    def __call__(self, value):
        if value is not CacheMissError and not isinstance(value, ExceptionWrapper):
            return self.callback(value)

@cython.freelist(100)
cdef class MissCallback:
    cdef object callback, __weakref__
    def __cinit__(self, callback):
        self.callback = callback
    def __call__(self, value):
        if value is CacheMissError:
            return self.callback(value)

@cython.freelist(100)
cdef class ExceptionCallback:
    cdef object callback, __weakref__
    def __cinit__(self, callback):
        self.callback = callback
    def __call__(self, value):
        if isinstance(value, ExceptionWrapper):
            return self.callback(value)

@cython.freelist(100)
cdef class AnyCallback:
    cdef object on_value, on_miss, on_exc, __weakref__
    def __cinit__(self, on_value, on_miss, on_exc):
        self.on_value = on_value
        self.on_miss = on_miss
        self.on_exc = on_exc
    def __call__(self, value):
        if value is CacheMissError:
            if self.on_miss is not None:
                return self.on_miss()
        elif isinstance(value, ExceptionWrapper):
            if self.on_exc is not None:
                return self.on_exc((<ExceptionWrapper>value).value)
        else:
            if self.on_value is not None:
                return self.on_value(value)

@cython.freelist(100)
cdef class DoneCallback:
    cdef object callback, __weakref__
    def __cinit__(self, callback):
        self.callback = callback
    def __call__(self, value):
        return self.callback()

cdef class NONE:
    pass

@cython.freelist(100)
cdef class Future:
    def __cinit__(self, logger = None):
        self._cb = None
        self._value = NONE
        self._logger = logger
        self._done_event = None
        self._running = 0
        self._cancel_pending = 0
        self._cancelled = 0

    cpdef _set_nothreads(self, value):
        """
        Like set(), but assuming no threading is involved. It won't wake waiting threads,
        nor will it try to be thread-safe. Safe to call when the calling
        thread is the only one owning references to this future, and much faster.
        """
        cdef object old, cbs
        
        if self._value is not NONE:
            # No setting twice
            return

        old = self._value # avoid deadlocks due to finalizers
        if self._cb is not None: # start atomic op
            cbs = list(self._cb) 
            self._value = value 
        else:
            cbs = None
            self._value = value # end atomic op
        old = None

        if cbs is not None:
            for cb in cbs:
                try:
                    cb(value)
                except:
                    if self._logger is not None:
                        error = self._logger
                    else:
                        error = logging.error
                    error("Error in async callback", exc_info = True)
        self._running = 0
    
    cpdef set(self, value):
        """
        Set the future's result as either a value, an exception wrappedn in ExceptionWrapper, or
        a cache miss if given CacheMissError (the class itself)
        """
        self._set_nothreads(value)

        if self._done_event is not None:
            # wake up waiting threads
            self._done_event.set()

    def set_result(self, value):
        self.set(value)

    def miss(self):
        """
        Shorthand for setting a cache miss result
        """
        self.set(CacheMissError)

    def _miss_nothreads(self):
        """
        Shorthand for setting a cache miss result without thread safety.
        See _set_nothreads
        """
        self._set_nothreads(CacheMissError)

    def exc(self, exc_info):
        """
        Shorthand for setting an exception result from an exc_info tuple
        as returned by sys.exc_info()
        """
        self.set(ExceptionWrapper.__new__(ExceptionWrapper, exc_info))

    def _exc_nothreads(self, exc_info):
        """
        Shorthand for setting an exception result from an exc_info tuple
        as returned by sys.exc_info(), without thread safety. 
        See _set_nothreads
        """
        self._set_nothreads(ExceptionWrapper.__new__(ExceptionWrapper, exc_info))

    def set_exception(self, exception):
        """
        Set the Future's exception object.
        """
        self.exc((type(exception),exception,None))

    def on_value(self, callback):
        """
        When and if the operation completes without exception, the callback 
        will be invoked with its result.
        """
        return self._on_stuff(ValueCallback.__new__(ValueCallback, callback))

    def on_miss(self, callback):
        """
        If the operation results in a cache miss, the callback will be invoked
        without arugments.
        """
        return self._on_stuff(MissCallback.__new__(MissCallback, callback))

    def on_exc(self, callback):
        """
        If the operation results in an exception, the callback will be invoked
        with an exc_info tuple as returned by sys.exc_info.
        """
        return self._on_stuff(ExceptionCallback.__new__(ExceptionCallback, callback))

    def on_any(self, on_value = None, on_miss = None, on_exc = None):
        """
        Handy method to set callbacks for all kinds of results, and it's actually
        faster than calling on_X repeatedly. None callbacks will be ignored.
        """
        return self._on_stuff(AnyCallback.__new__(AnyCallback, on_value, on_miss, on_exc))

    def on_any_once(self, on_value = None, on_miss = None, on_exc = None):
        """
        Like on_any, but will only set the callback if no other callback has been set
        """
        if self._cb is None:
            return self._on_stuff(AnyCallback.__new__(AnyCallback, on_value, on_miss, on_exc))

    def on_done(self, callback):
        """
        When the operation is done, the callback will be invoked without arguments,
        regardless of the outcome. If the operation is cancelled, it won't be invoked.
        """
        return self._on_stuff(DoneCallback.__new__(DoneCallback, callback))

    def chain(self, defer):
        """
        Invoke all the callbacks of the other defer
        """
        self._on_stuff(defer.set)

    def chain_std(self, defer):
        """
        Invoke all the callbacks of the other defer, without assuming the other
        defer follows our non-standard interface.
        """
        return self.on_any(
            defer.set_result,
            functools_partial(defer.set_exception, CacheMissErrorCached),
            DeferExceptionCallback.__new__(DeferExceptionCallback, defer)
        )

    cpdef _on_stuff(self, callback):
        if self._value is NONE:
            if self._cb is None:
                self._cb = list()
            self._cb.append(callback)
        else:
            callback(self._value)
        return self

    def add_done_callback(self, callback):
        """
        When the operatio is done, the callback will be invoked with the
        future object as argument.
        """
        return self._on_stuff(WeakCallback.__new__(WeakCallback, self, callback))

    cdef int c_done(self) except -1:
        """
        Return True if the operation has finished, in a result or exception or cancelled, and False if not.
        """
        if self._value is not NONE or self._cancelled:
            return 1
        else:
            return 0

    def done(self):
        """
        Return True if the operation has finished, in a result or exception or cancelled, and False if not.
        """
        if self.c_done():
            return True
        else:
            return False

    def running(self):
        """
        Return True if the operation is running and cannot be cancelled. False if not running
        (yet or done).
        """
        if self._running:
            return True
        else:
            return False

    def cancelled(self):
        """
        Return True if the operation has been cancelled successfully.
        """
        if self._cancelled:
            return True
        else:
            return False

    def cancel_pending(self):
        """
        Return True if cancel was called.
        """
        if self._cancel_pending:
            return True
        else:
            return False

    def cancel(self):
        """
        Request cancelling of the operation. If the operation cannot be cancelled,
        it will return False. Otherwise, it will return True.
        """
        if self._cancelled:
            return False
        else:
            self._cancel_pending = 1
            return True

    def set_running_or_notify_cancelled(self):
        """
        To be invoked by executors before executing the operation. If it returns True,
        the operation may go ahead, and if False, a cancel has been requested and the
        operation should not be initiated, all threads waiting for the operation will
        be wakened immediately and the future will be marked as cancelled.
        """
        if self._cancel_pending:
            self._cancelled = 1
            self._running = 0

            # Notify waiters and callbacks
            self.set_exception(CancelledErrorCached) 
            
            return False
        else:
            self._running = 1
            return True

    cdef c_result(self, timeout, int norecurse):
        """
        Return the operation's result, if any. If an exception was the result, re-raise it.
        If it was cancelled, raises CancelledError, and if timeout is specified and not None,
        and the specified time elapses without a result available, raises TimeoutError.
        """
        cdef object value
        cdef ExceptionWrapper exc_value
        
        if self._value is not NONE:
            value = self._value
            if isinstance(value, ExceptionWrapper):
                exc_value = <ExceptionWrapper>value
                raise exc_value.value[0], exc_value.value[1], exc_value.value[2]
            elif value is CacheMissError:
                raise CacheMissErrorCached
            else:
                return value
        elif self._cancelled:
            raise CancelledErrorCached
        else:
            if timeout is not None and timeout == 0:
                raise TimeoutErrorCached
            else:
                # Wait for it
                if self._done_event is None:
                    self._done_event = threading.Event()
                if self._done_event.wait(timeout) and not norecurse:
                    return self.c_result(0, 1)
                elif self._cancelled:
                    raise CancelledErrorCached
                else:
                    raise TimeoutErrorCached

    def result(self, timeout=None, norecurse=False):
        """
        Return the operation's result, if any. If an exception was the result, re-raise it.
        If it was cancelled, raises CancelledError, and if timeout is specified and not None,
        and the specified time elapses without a result available, raises TimeoutError.
        """
        return self.c_result(timeout, norecurse)

    def exception(self, timeout=None):
        """
        If the operation resulted in an exception, return the exception object.
        Otherwise, return None. If the operation has been cancelled, raises CancelledError,
        and if timeout is specified and not None, and the specified time elapses without 
        a result available, raises TimeoutError.
        """
        cdef object value
        cdef ExceptionWrapper exc_value
        
        if self._value is not NONE:
            value = self._value
            if isinstance(value, ExceptionWrapper):
                exc_value = <ExceptionWrapper>value
                return exc_value.value[1] or exc_value.value[0]
            elif value is CacheMissError:
                return CacheMissError
            else:
                return None
        elif self._cancelled:
            raise CancelledErrorCached
        else:
            try:
                self.c_result(timeout, 0)
                return None
            except CancelledError:
                raise
            except Exception,e:
                return e
        
