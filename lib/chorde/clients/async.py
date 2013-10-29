from chorde.serialize import serialize

import logging
import time
import weakref
import functools
import thread
import threading
import operator
import sys

# No need for real multiprocessing. In fact, using real
# multiprocessing would force pickling of values, which would be
# undesirable, pushing pickling cost into foreground threads.
import multiprocessing.dummy
from multiprocessing.pool import ThreadPool

from .base import BaseCacheClient, CacheMissError, NONE

try:
    from concurrent.futures import TimeoutError, CancelledError
except ImportError:
    class TimeoutError(Exception):
        pass
    class CancelledError(Exception):
        pass

class _NONE:pass
class _DELETE:pass
class _EXPIRE:pass
class _PURGE:pass
class _CLEAR:pass

class Defer(object):
    """
    Wrap a callable in this, and pass it as a value to an AsyncWriteCacheClient,
    and the evaluation of the given callable will happen asynchronously. The cache
    will return stale entries until the callable is finished computing a new value.

    If a future attribute is attached, this defer will act as executioner of that
    future, and both set_running_or_notify_cancelled and set_result or set_exception
    will be called on it.

    If a future is attached during execution, set_running_or_notify_cancelled will
    not be invoked, but set_result will.
    """
    
    def __init__(self, callable_, *args, **kwargs):
        self.callable_ = callable_
        self.args = args
        self.kwargs = kwargs
        self.lazy = False

    def undefer(self, getattr=getattr):
        future = getattr(self, 'future', None)
        if future is None or future.set_running_or_notify_cancelled():
            try:
                self.rv = rv = self.callable_(*self.args, **self.kwargs)
            except:
                future = getattr(self, 'future', None)
                if future is not None:
                    future.exc(sys.exc_info())
                raise
            return rv
        else:
            return _NONE

    def done(self, getattr=getattr):
        future = getattr(self, 'future', None)
        if future is not None and not future.done():
            future.set(getattr(self, 'rv', None))

class AsyncCacheWriterPool(ThreadPool):
    def __init__(self, size, workers, client, overflow = False):
        # This patches ThreadPool, which is broken when instanced 
        # from inside a DummyThread (happens after forking)
        current = multiprocessing.dummy.current_process()
        if not hasattr(current, '_children'):
            current._children = weakref.WeakKeyDictionary()
        
        self.client = client
        self.logger = logging.getLogger("AsyncCache")
        self.size = size
        self.workers = workers
        
        # queueset holds the values to be written, associated
        # by key, providing some write-back coalescense in
        # high-load environments
        self.queueset = {}
        self.workset = {}
        self.threadset = set()
        self.done_event = threading.Event()
        self.overflow = overflow
        
        ThreadPool.__init__(self, workers)

    @staticmethod
    def _writer(self, key):
        # self is weakref
        self = self()
        if self is None:
            return

        thread_id = thread.get_ident()
        if thread_id not in self.threadset:
            self.threadset.add(thread_id)
        else:
            thread_id = None

        ev = self.done_event
        deferred = _NONE
        value, ttl = self.dequeue(key)

        try:
            if value is _NONE or value is NONE:
                # Something's hinky
                return
            elif isinstance(value, Defer):
                deferred = value
                try:
                    value = value.undefer()
                except:
                    self.logger.error("Error in background cache refresh", exc_info=True)
                    value = _NONE
            
            if value is _NONE or value is NONE:
                # undefer probably decided not to compute anything (or an error arose, whatever)
                if deferred is not _NONE:
                    deferred.done()
                return
                
            elif value is _DELETE:
                try:
                    self.client.delete(key)
                except:
                    self.logger.error("Error deleting key", exc_info=True)
            
            elif value is _EXPIRE:
                try:
                    self.client.expire(key)
                except:
                    self.logger.error("Error expiring key", exc_info=True)
            
            elif value is _CLEAR:
                try:
                    self.client.clear()
                except:
                    self.logger.error("Error clearing cache", exc_info = True)
            
            elif value is _PURGE:
                try:
                    self.client.purge()
                except:
                    self.logger.error("Error purging cache", exc_info = True)
            
            else:
                try:
                    self.client.put(key, value, ttl)
                except:
                    self.logger.error("Error saving data in cache", exc_info=True)

            if deferred is not _NONE:
                deferred.done()
        finally:
            # Signal waiting threads
            w = self.workset.pop(key, None)
            if w is not None:
                kev = w[2]
            else:
                kev = None
            del w
            
            if thread_id is not None and thread_id not in map(operator.itemgetter(0), self.workset.values()):
                try:
                    self.threadset.remove(thread_id)
                except KeyError:
                    pass
            
            ev.set()
            if kev is not None:
                kev.set()
        
    @property
    def capacity(self):
        return self.size

    @property
    def usage(self):
        return len(self.queueset)

    @serialize
    def dequeue(self, key):
        rv = self.queueset.pop(key, _NONE)
        self.workset[key] = thread.get_ident(), rv, threading.Event()
        return rv

    @serialize
    def drop_one(self):
        for key in self.queueset:
            break
        else:
            return
        rv = self.queueset.pop(key, _NONE)
        return rv

    def enqueue(self, key, value, ttl=None):
        if thread.get_ident() in self.threadset:
            # Oops, recursive call, bad idea
            # Run inline
            self.queueset[key] = value, ttl
            self._writer(weakref.ref(self), key)
        else:
            if key not in self.queueset:
                if self.overflow:
                    # use overflow semantics: remove old entries to make room for new ones
                    if len(self.queueset) >= self.size:
                        # just two, tit-for-tat, one in, two out. Avoids large latencies,
                        # and guarantees stable sizes, around, while not strictly below "size"
                        for _ in xrange(2):
                            self.drop_one()
                            if len(self.queueset) < self.size:
                                break
                else:
                    # blocking semantics, wait
                    ev = self.done_event
                    while len(self.queueset) >= self.size:
                        ev.wait(1.0)
                        if ev.isSet():
                            ev.clear()
            delayed = self._enqueue(key, value, ttl)
            if delayed is not None:
                # delayed callback, invoke now that we're outside the critical section
                delayed()

    @serialize
    def clearqueue(self):
        self.queueset.clear()
    
    @serialize
    def _enqueue(self, key, value, ttl, isinstance=isinstance, getattr=getattr):
        delayed = None
        queueset = self.queueset
        workset = self.workset
        if key not in queueset:
            if not isinstance(value, Defer) or key not in workset:
                queueset[key] = value, ttl
                self.apply_async(self._writer, (weakref.ref(self), key))
            else:
                # else, bad luck, we assume defers compute, so if two
                # defers go in concurrently, only the first will be invoked,
                # instead of the last - the first cannot be canceled after all,
                # and we want to only invoke one. So no choice.
                
                # ...if the defer has a future attached
                future = getattr(value, 'future', None)
                if future is not None:
                    # we do have to hook into the future though... 
                    working = workset.get(key)
                    if working is not None:
                        working = working[1]
                        if working is not _NONE:
                            working = working[0]
                        if isinstance(working, Defer):
                            working_future = getattr(working, 'future', None)
                            if working_future is not None:
                                delayed = functools.partial(working_future.chain, future)
                            else:
                                working.future = future
                        else:
                            # Delay the callback, we're in a critical section here
                            delayed = functools.partial(future.set, working)
        else:
            if isinstance(value, Defer):
                future = getattr(value, 'future', None)
                if future is not None:
                    queue_value = queueset.get(key)
                    if queue_value is not None:
                        # Ok, they'll wanna get the value when it's done
                        queue_value = queue_value[0]
                        if isinstance(queue_value, Defer):
                            queue_future = getattr(queue_value, 'future', None)
                            if queue_future is not None:
                                delayed = functools.partial(queue_future.chain, future)
                            else:
                                queue_value.future = future
                        else:
                            # Why not
                            delayed = functools.partial(future.set, queue_value)
            queueset[key] = value, ttl
        return delayed
    
    def waitkey(self, key, timeout=None):
        if thread.get_ident() in self.threadset:
            # Oops, recursive call, bad idea
            return
        elif timeout is None:
            while self.contains(key):
                ev = self.workset.get(key)
                if ev is not None:
                    ev = ev[2]
                if ev is not None:
                    ev.wait(1.0)
                else:
                    break
        else:
            tfin = time.time() + timeout
            while self.contains(key) and tfin >= time.time():
                ev = self.workset.get(key)
                if ev is not None:
                    ev = ev[2]
                if ev is not None:
                    ev.wait(min(1.0, timeout))
                else:
                    break
                timeout = tfin - time.time()
    
    def getTtl(self, key, default = None):
        return self.queueset.get(key, default)
    
    def contains(self, key):
        # Not atomic, but it doesn't really matter much, very unlikely and benignly to fail
        if key in self.queueset:
            return True
        else:
            # Exclude keys being computed by this thread, such calls 
            # want the async wrapper out of their way
            tid = thread.get_ident()
            return self.workset.get(key, (tid,))[0] != tid

    def put(self, key, value, ttl):
        self.enqueue(key, value, ttl)

    def delete(self, key):
        self.enqueue(key, _DELETE)

    def expire(self, key):
        self.enqueue(key, _EXPIRE)

    def clear(self):
        self.clearqueue()
        self.enqueue(_CLEAR, _CLEAR)

    def purge(self):
        self.enqueue(_PURGE, _PURGE)
    
class AsyncWriteCacheClient(BaseCacheClient):
    def __init__(self, client, writer_queue_size, writer_workers, overflow = False):
        self.client = client
        self.writer_queue_size = writer_queue_size
        self.writer_workers = writer_workers
        self.writer = None
        self.overflow = overflow
        self.spawning_lock = threading.Lock()
        
    def assert_started(self):
        if self.writer is None:
            with self.spawning_lock:
                if self.writer is None:
                    self.writer = AsyncCacheWriterPool(
                        self.writer_queue_size, 
                        self.writer_workers,
                        self.client,
                        self.overflow)
    
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
    
    @property
    def async(self):
        return True
    
    @property
    def capacity(self):
        return (self.client.capacity, self.writer.capacity if self.writer is not None else 0)

    @property
    def usage(self):
        return (self.client.usage, self.writer.usage if self.writer is not None else 0)

    def put(self, key, value, ttl):
        self.assert_started()
        self.writer.put(key, value, ttl)
    
    def delete(self, key):
        self.assert_started()
        self.writer.delete(key)

    def expire(self, key):
        self.assert_started()
        self.writer.expire(key)

    def clear(self):
        if self.is_started():
            self.writer.clear()

    def purge(self):
        if self.is_started():
            self.writer.purge()
    
    def getTtl(self, key, default = NONE, **kw):
        ettl = None
        if self.is_started():
            # Try to read pending writes as if they were on the cache
            value = self.writer.getTtl(key, _NONE)
            if value is not _NONE:
                value, ttl = value
                if value is _DELETE:
                    # Deletion means a miss... right?
                    if default is NONE:
                        raise CacheMissError, key
                    else:
                        return default, -1
                elif value is _EXPIRE:
                    # Expiration just sets the TTL
                    ettl = -1
                elif not isinstance(value, Defer):
                    return value, ttl
            # Yep, _NONE when querying the writer, because we don't want
            # to return a default if the writer doesn't have it, we must
            # still check the client.
        
        # Ok, read the cache then
        value, ttl = self.client.getTtl(key, default, **kw)
        if ettl is not None:
            ttl = ettl
        if value is NONE:
            raise CacheMissError, key
        else:
            return value, ttl
    
    def wait(self, key, timeout = None):
        if self.writer.contains(key):
            self.writer.waitkey(key, timeout)
    
    def contains(self, key, ttl = None, **kw):
        if self.is_started():
            if self.writer.contains(key):
                return True
            else:
                return self.client.contains(key, ttl, **kw)
        else:
            return self.client.contains(key, ttl, **kw)

class ExceptionWrapper(object):
    __slots__ = ('value',)

    def __init__(self, value):
        self.value = value

class Future(object):
    __slots__ = (
        '_cb', '_value', '_logger', '_running', '_cancel_pending', '_cancelled', '_done_event', 
        '_lock', '__weakref__'
    )
    
    def __init__(self, logger = None):
        self._cb = []
        self._logger = logger
        self._lock = threading.Lock()
    
    def set(self, value):
        """
        Set the future's result as either a value, an exception wrappedn in ExceptionWrapper, or
        a cache miss if given CacheMissError (the class itself)
        """
        with self._lock:
            cbs = list(self._cb)
            self._value = value
        
        for cb in cbs:
            try:
                cb(value)
            except:
                if self._logger is not None:
                    error = self._logger
                else:
                    error = logging.error
                error("Error in async callback", exc_info = True)
        self._running = False
        
        event = getattr(self, '_done_event', None)
        if event is not None:
            # wake up waiting threads
            event.set()

    set_result = set

    def miss(self):
        """
        Shorthand for setting a cache miss result
        """
        self.set(CacheMissError)

    def exc(self, exc_info):
        """
        Shorthand for setting an exception result from an exc_info tuple
        as returned by sys.exc_info()
        """
        self.set(ExceptionWrapper(exc_info))

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
        def value_callback(value):
            if value is not CacheMissError and not isinstance(value, ExceptionWrapper):
                return callback(value)
        return self._on_stuff(value_callback)

    def on_miss(self, callback):
        """
        If the operation results in a cache miss, the callback will be invoked
        without arugments.
        """
        def miss_callback(value):
            if value is CacheMissError:
                return callback()
        return self._on_stuff(miss_callback)

    def on_exc(self, callback):
        """
        If the operation results in an exception, the callback will be invoked
        with an exc_info tuple as returned by sys.exc_info.
        """
        def exc_callback(value):
            if isinstance(value, ExceptionWrapper):
                return callback(value.value)
        return self._on_stuff(exc_callback)

    def on_any(self, on_value = None, on_miss = None, on_exc = None):
        """
        Handy method to set callbacks for all kinds of results, and it's actually
        faster than calling on_X repeatedly. None callbacks will be ignored.
        """
        def callback(value):
            if value is CacheMissError:
                if on_miss is not None:
                    return on_miss()
            elif isinstance(value, ExceptionWrapper):
                if on_exc is not None:
                    return on_exc(value.value)
            else:
                if on_value is not None:
                    return on_value(value)
        return self._on_stuff(callback)

    def on_done(self, callback):
        """
        When the operation is done, the callback will be invoked without arguments,
        regardless of the outcome. If the operation is cancelled, it won't be invoked.
        """
        def done_callback(value):
            return callback()
        return self._on_stuff(done_callback)

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
            functools.partial(defer.set_exception, CacheMissError()),
            lambda value : defer.set_exception(value[1] or value[0])
        )

    def _on_stuff(self, callback, hasattr=hasattr):
        cbap = self._cb.append
        docall = hasattr(self, '_value')
        if not docall:
            with self._lock:
                docall = hasattr(self, '_value')
                cbap(callback)
        if docall:
            callback(self._value)
        return self

    def add_done_callback(self, callback):
        """
        When the operatio is done, the callback will be invoked with the
        future object as argument.
        """
        me = weakref.ref(self)
        def weak_callback(value):
            self = me()
            if self is not None:
                return callback(self)
        return self._on_stuff(weak_callback)

    def done(self, hasattr=hasattr):
        """
        Return True if the operation has finished, in a result or exception, and False if not.
        """
        return hasattr(self, '_value')

    def running(self, getattr=getattr):
        """
        Return True if the operation is running and cannot be cancelled. False if not running
        (yet or done).
        """
        return getattr(self, '_running', False)

    def cancelled(self, getattr=getattr):
        """
        Return True if the operation has been cancelled successfully.
        """
        return getattr(self, '_cancelled', False)

    def cancel_pending(self, getattr=getattr):
        """
        Return True if cancel was called.
        """
        return getattr(self, '_cancel_pending', False)

    def cancel(self, getattr=getattr):
        """
        Request cancelling of the operation. If the operation cannot be cancelled,
        it will return False. Otherwise, it will return True.
        """
        if getattr(self, '_cancelled', False):
            return False
        else:
            self._cancel_pending = True
            return True

    def set_running_or_notify_cancelled(self, getattr=getattr):
        """
        To be invoked by executors before executing the operation. If it returns True,
        the operation may go ahead, and if False, a cancel has been requested and the
        operation should not be initiated, all threads waiting for the operation will
        be wakened immediately and the future will be marked as cancelled.
        """
        if getattr(self, '_cancel_pending', False):
            self._cancelled = True
            self._running = False
            event = getattr(self, '_done_event', None)
            if event is not None:
                event.set()
            return False
        else:
            self._running = True
            return True

    def result(self, timeout=None, hasattr=hasattr, getattr=getattr, norecurse=False):
        """
        Return the operation's result, if any. If an exception was the result, re-raise it.
        If it was cancelled, raises CancelledError, and if timeout is specified and not None,
        and the specified time elapses without a result available, raises TimeoutError.
        """
        if hasattr(self, '_value'):
            value = self._value
            if isinstance(value, ExceptionWrapper):
                raise value.value[0], value.value[1], value.value[2]
            elif value is CacheMissError:
                raise CacheMissError
            else:
                return self._value
        elif self.cancelled():
            raise CancelledError
        else:
            if timeout == 0:
                raise TimeoutError
            else:
                # Wait for it
                event = getattr(self, '_done_event', None)
                if event is None:
                    event = self._done_event = threading.Event()
                # First loop eagerly waits on the recently-created event
                # Second loop grabs the instance event (which could have been
                # clobbered by another thread). This is lockless yet safe,
                # and quick on the most common condition (no contention)
                for timeout in (0, timeout):
                    if event.wait(timeout) and not norecurse:
                        return self.result(0, norecurse=True)
                    elif self.cancelled():
                        raise CancelledError
                    else:
                        time.sleep(0) # < give other threads a chance
                        event = self._done_event
                else:
                    raise TimeoutError

    def exception(self, timeout=None):
        """
        If the operation resulted in an exception, return the exception object.
        Otherwise, return None. If the operation has been cancelled, raises CancelledError,
        and if timeout is specified and not None, and the specified time elapses without 
        a result available, raises TimeoutError.
        """
        if hasattr(self, '_value'):
            value = self._value
            if isinstance(value, ExceptionWrapper):
                return value.value[1] or value.value[0]
            elif value is CacheMissError:
                return CacheMissError
            else:
                return None
        elif self.cancelled():
            raise CancelledError
        else:
            try:
                self.result()
                return None
            except CancelledError:
                raise
            except Exception,e:
                return e
        

def makeFutureWrapper(base):
    """
    Nice utility function to create Future wrappers. If using a library
    with concurrent-compatible futures, but not quite the same, and the library
    checks for inheritance instead of relying on duck typing, then you'll
    need to make such a class and wrap chorde's futures with it.
    """
    class WrapperFuture(base):
        def __init__(self, wrapped):
            self.__wrapped = wrapped

        for name, fn in vars(base).iteritems():
            if not name.startswith('__') and callable(fn):
                def mkf(name, fn):
                    @functools.wraps(fn)
                    def f(self, *p, **kw):
                        return getattr(self.__wrapped, name)(*p, **kw)
                    return f
                locals()[name] = mkf(name, fn)
        del name, fn
    return WrapperFuture

class AsyncCacheProcessor(object):
    """
    An async cache processor will allow asynchronous reads
    and writes to a cache, Efficiently fitting into an async
    framework by passing and invoking callbacks.

    It modifies the cache interface to return a Future
    instead of a value, upon which an on_value(callback)
    method will retrieve the result, if any.

    If there is a cache miss, on_miss callbacks will be invoked 
    instead, and in case of an exception, on_exc.

    Futures also implement Python 3's concurrent.futures.Future
    interface, see Future's documentation for more details.

    It also provides a do_async, that lets you dump arbitrary
    tasks on this processor's async processing pool (in case
    you need to do it for synchronization)
    """
    def __init__(self, workers, client):
        # This patches ThreadPool, which is broken when instanced 
        # from inside a DummyThread (happens after forking)
        current = multiprocessing.dummy.current_process()
        if not hasattr(current, '_children'):
            current._children = weakref.WeakKeyDictionary()
        
        self.client = client
        self.logger = logging.getLogger("AsyncCache")
        self.workers = workers
        self._threadpool = None
        self._spawnlock = threading.Lock()

    @property
    def threadpool(self):
        if self._threadpool is None:
            with self._spawnlock:
                if self._threadpool is None:
                    self._threadpool = ThreadPool(self.workers)
        return self._threadpool

    def _enqueue(self, action):
        future = Future(logger=self.logger)
        def wrapped_action():
            if future.set_running_or_notify_cancelled():
                try:
                    future.set(action())
                except CacheMissError:
                    future.miss()
                except:
                    # Clear up traceback to avoid leaks
                    future.exc(sys.exc_info()[:-1] + (None,))
        self.threadpool.apply_async(wrapped_action, ())
        return future

    @property
    def capacity(self):
        return self.client.capacity

    @property
    def usage(self):
        return self.client.usage

    def do_async(self, func, *args, **kwargs):
        return self._enqueue(functools.partial(func, *args, **kwargs))

    def bound(self, client):
        """
        Returns a proxy of this processor bound to the specified client instead.
        """
        return WrappedCacheProcessor(self, client)
    
    def getTtl(self, key, default = NONE, **kw):
        return self._enqueue(functools.partial(self.client.getTtl, key, default, **kw))
    
    def get(self, key, default = NONE):
        return self._enqueue(functools.partial(self.client.get, key, default))
    
    def contains(self, key, *p, **kw):
        return self._enqueue(functools.partial(self.client.contains, key, *p, **kw))

    def put(self, key, value, ttl):
        return self._enqueue(functools.partial(self.client.put, key, value, ttl))

    def add(self, key, value, ttl):
        return self._enqueue(functools.partial(self.client.add, key, value, ttl))

    def delete(self, key):
        return self._enqueue(functools.partial(self.client.delete, key))

    def expire(self, key):
        return self._enqueue(functools.partial(self.client.expire, key))

    def clear(self):
        return self._enqueue(self.client.clear)

    def purge(self):
        return self._enqueue(self.client.purge)
    

class WrappedCacheProcessor(object):
    """
    Wraps an AsyncCacheProcessor, binding its interface to a
    different client.
    """
    def __init__(self, processor, client):
        self.processor = processor
        self.client = client

    @property
    def capacity(self):
        return self.client.capacity

    @property
    def usage(self):
        return self.client.usage

    def do_async(self, func, *args, **kwargs):
        return self.processor.do_async(func, *args, **kwargs)

    def bound(self, client):
        if client is self.client:
            return self
        else:
            return WrappedCacheProcessor(self.processor, client)
    
    def getTtl(self, key, default = None, **kw):
        return self.processor.do_async(self.client.getTtl, key, default, **kw)
    
    def get(self, key, default = NONE):
        return self.processor.do_async(self.client.get, key, default)
    
    def contains(self, key, *p, **kw):
        return self.processor.do_async(self.client.contains, key, *p, **kw)

    def put(self, key, value, ttl):
        return self.processor.do_async(self.client.put, key, value, ttl)

    def add(self, key, value, ttl):
        return self.processor.do_async(self.client.add, key, value, ttl)

    def delete(self, key):
        return self.processor.do_async(self.client.delete, key)

    def expire(self, key):
        return self.processor.do_async(self.client.expire, key)

    def clear(self):
        return self.processor.do_async(self.client.clear)

    def purge(self):
        return self.processor.do_async(self.client.purge)
    
