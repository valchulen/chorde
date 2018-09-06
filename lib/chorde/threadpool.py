# -*- coding: utf-8 -*-
import collections
import functools
import itertools
import logging
import multiprocessing
import os
import sys
import thread
import threading
import time
import weakref

import worker

class TimeoutError(Exception):
    pass
class TerminateWorker(Exception):
    pass

class WorkerThread(threading.Thread):
    def __init__(self,target,*args,**kwargs):
        threading.Thread.__init__(self, name=kwargs.pop('name', None))
        self.target = target
        self.args = args
        self.kwargs = kwargs
        self.__terminate = False

    def run(self):
        while not self.__terminate:
            try:
                try:
                    self.target(*self.args,**self.kwargs)
                finally:
                    worker._callCleanupHooks()
            except TerminateWorker:
                self.logger.info("Worker terminated")
                self.terminate(False)
            except Exception:
                self.logger.error("Exception ocurred in worker thread:", exc_info = True)

    def start(self):
        global _nothreads
        threading.Thread.start(self)

    def terminate(self, wait = True):
        self.__terminate = True
        if wait:
            self.join()

try:
    from clients._async import ExceptionWrapper
except ImportError:
    class ExceptionWrapper(object):  # lint:ok
        __slots__ = ('exc',)

        def __init__(self, exc):
            self.exc = exc

        def reraise(self):
            exc = self.exc
            del self.exc
            raise exc[0], exc[1], exc[2]

class WaitIter:
    def __init__(self, event, queues, timeout = None):
        self.event = event
        self.queues = queues
        self.timeout = timeout
        self._terminate = False
    def terminate(self):
        self._terminate = True
        self.event.set()
    def __iter__(self):
        return self
    def next(self):
        if self._terminate:
            threading.current_thread().terminate(False)
        self.event.wait(self.timeout)
        raise StopIteration

class ThreadPool:
    """
    Re-implementation of multiprocessing.pool.ThreadPool optimized for threads
    and asynchronous result-less tasks.
    Implements quasi-lockless double-buffering so queue insertions are very fast, and
    multiple queues for managing fairness.

    This implementation is forcibly a daemon thread pool, which when destroyed
    will cancel all pending tasks, and no task returns any result, and has been
    optimized for that usage pattern.
    """

    Process = WorkerThread

    def __init__(self, workers = None, min_batch = 10, max_batch = 1000, max_slice = None, logger = None,
            name_pattern = None):
        if workers is None:
            workers = multiprocessing.cpu_count()

        self.workers = workers
        self.name_pattern = name_pattern
        self.logger = logger if logger is not None else logging.getLogger('chorde')
        self.__last_worker_index = 1
        self.__workers = None
        self.__pid = os.getpid()
        self.__spawnlock = threading.Lock()
        self.__swap_lock = threading.Lock()
        self.__not_empty = threading.Event()
        self.__empty = threading.Event()
        self.__empty.set()
        self.__cleanup_callbacks = []

        self.local = threading.local()
        self.queues = collections.defaultdict(list)
        self.queue_weights = {}
        self.completed_tasks = 0
        self.__queue_slices = {}
        self.__worklen = 0
        self.__workset = set()
        self.__busyqueues = set()
        self.__busyfactors = {}
        self.__exhausted_iter = WaitIter(self.__not_empty, self.queues)
        self.__dequeue = self.__exhausted = self.__exhausted_iter.next

        self.min_batch = min_batch
        self.max_batch = max_batch
        self.max_slice = max_slice

    def queuelen(self, queue = None):
        return (
            len(self.queues.get(queue,()))
            - self.__queue_slices.get(queue,0)
            + int(self.__worklen * self.__busyfactors.get(queue,0))
        )

    # alias for multiprocessing.pool compatibility
    qsize = queuelen

    # alias for multiprocessing.pool compatibility
    @property
    def _taskqueue(self):
        return self

    def queueprio(self, queue = None):
        return self.queue_weights.get(queue,1)

    def set_queueprio(self, prio, queue = None):
        self.queue_weights[queue] = prio

    def register_cleanup_callback(self, callback):
        self.__cleanup_callbacks.append(callback)

    def _call_cleanup_callbacks(self):
        for callback in self.__cleanup_callbacks:
            try:
                callback()
            except:
                self.logger.error("Error in task cleanup callback", exc_info = True)

    def __swap_queues(self, max=max, min=min, len=len):
        queues = self.queues
        qpop = queues.pop
        qget = queues.get
        queue_slices = self.__queue_slices
        pget = queue_slices.get
        ppop = queue_slices.pop
        qprio = self.queue_weights.get
        qnames = queues.keys()
        wqueues = []
        wprios = []
        wposes = []
        iquantities = {}
        itotal = 0
        can_straggle = False

        if qnames:
            # Compute batch size
            # Must be fair, so we must calibrate the batch ends
            # with all queues more or less at the same time
            # Allow some unfairness (but only some)
            # Slices are zero-copy (we don't want to copy a lot of stuff)
            # until maxbatch (we don't want race coditions that result in infinite growth)
            # maxbatch, if None, means "half the queue" (which auto-amortizes the cost of slicing)
            min_batch = self.min_batch
            max_batch = self.max_batch
            max_slice = self.max_slice
            qslots = min(max_batch, max(min_batch,min([len(qget(q)) / qprio(q,1) for q in qnames])))
            for qname in qnames:
                q = qget(qname)
                qpos = pget(qname,0)
                prio = qprio(qname,1)
                margin = max(prio,min_batch)
                batch = qslots * prio
                if batch >= (len(q) - margin - qpos):
                    #print "move %s" % (qname,)
                    q = qpop(qname)
                    if qpos:
                        del q[:qpos] # atomic re. pushes
                        ppop(qname,None) # reset
                    wqueues.append(q)
                    wposes.append(0)
                    qlen = len(q)
                    iquantities[qname] = qlen
                    itotal += qlen
                    can_straggle = True
                else:
                    if qpos > (max_slice or (len(q)/2)):
                        # copy-slicing
                        #print "copy-slice %s[%d:%d] of %d" % (qname,qpos,qpos+batch,len(q))
                        qslice = q[qpos:qpos+batch]
                        qlen = len(qslice)
                        iquantities[qname] = qlen
                        itotal += qlen
                        wqueues.append(qslice)
                        del q[:qpos+batch]
                        del qslice
                        ppop(qname,None)
                        wposes.append(0)
                    else:
                        # zero-copy slicing
                        #print "iter-slice %s[%d:%d] of %d" % (qname,qpos,qpos+batch,len(q))
                        qlen = min(batch, max(1, len(q) - qpos))
                        iquantities[qname] = qlen
                        itotal += qlen
                        wqueues.append(itertools.islice(q, qpos, qpos+batch)) # queue heads are immutable
                        queue_slices[qname] = qpos+batch
                        wposes.append(None)
                wprios.append(prio)

        if wqueues:
            self.__busyqueues.clear()
            self.__busyqueues.update(qnames)

            # Flatten with weights
            # Do it repeatedly to catch stragglers (those that straggle past the flattening step)
            iqueue = []
            iappend = iqueue.append
            islice = itertools.islice
            cycle = itertools.cycle
            izip = itertools.izip
            repeat = itertools.repeat
            partial = functools.partial
            retry = True

            while retry:
                if can_straggle:
                    # Wait for stragglers
                    time.sleep(0.0001)

                queues = []
                qposes = []
                for q,qprio,wpos in izip(wqueues, wprios, wposes):
                    if wpos is not None:
                        # must slice to make sure we take a stable snapshot of the list
                        # we'll process stragglers on the next iteration
                        qlen = len(q)
                        qiter = iter(islice(q,wpos,wpos+qlen))
                        qposes.append(wpos+qlen)
                    else:
                        qiter = iter(q)
                        qposes.append(None)
                    queues.append(partial(repeat, qiter.next, qprio))
                wposes = qposes

                ioffs = 0
                ilen = len(iqueue)
                while queues:
                    try:
                        for ioffs,q in islice(cycle(enumerate(queues)), ioffs, None):
                            for q in q():
                                iappend(q())
                    except StopIteration:
                        del queues[ioffs]
                retry = can_straggle and len(iqueue) != ilen
            self.__worklen = len(iqueue)
            self.__dequeue = iter(iqueue).next
            if itotal:
                ftotal = float(itotal)
                self.__busyfactors = dict([(qname, quant/ftotal) for qname,quant in iquantities.iteritems()])
            else:
                self.__busyfactors = {}
        elif self.__dequeue is not self.__exhausted:
            self.__not_empty.clear()
            self.__worklen = 0
            self.__busyfactors = {}
            self.__dequeue = self.__exhausted

            # Try again
            # This is a transition from working to empty, which means
            # until now, pushing threads didn't set the weakeup call event.
            # So, before actually sleeping, try again
            self.__swap_queues()
        elif self.__not_empty.isSet():
            # Try again
            # Someone may have sneaked in while we were in the above case
            # and may still neak in after we clear the not_empty event
            self.__not_empty.clear()
            self.__worklen = 0
            self.__busyfactors = {}
            self.__dequeue = self.__exhausted
            self.__swap_queues()
        else:
            # Still empty, can safely give up until signaled
            # Wake up threads trying to join
            self.__empty.set()

    def _dequeue(self):
        tid = thread.get_ident()
        workset = self.__workset
        while True:
            if self.__dequeue is self.__exhausted and not self.queues and not self.__not_empty.isSet():
                # Sounds like there's nothing to do
                # Yeah, gonna wait
                workset.discard(tid)
                if not workset and not self.queues:
                    self.__empty.set()
            else:
                workset.add(tid)
            try:
                rv = self.__dequeue()
                self.__worklen -= 1 # not atomic, but we don't care
                return rv
            except StopIteration:
                # Exhausted whole workqueue?
                with self.__swap_lock:
                    try:
                        if self.__dequeue is self.__exhausted:
                            # Pointless to wait, just swap again
                            raise StopIteration
                        else:
                            # Try it
                            workset.add(tid)
                            rv = self.__dequeue()
                            self.__worklen -= 1 # not atomic, but we don't care
                            return rv
                    except StopIteration:
                        # Yep, exhausted queue, build up new workqueue
                        self.__swap_queues()

    def _enqueue(self, queue, task):
        self.queues[queue].append(task)

        # Wake up waiting threads
        # Note that it's not necessary to invoke this all the time. If the
        # flags are the right way at any point within this function being
        # run, then it already means the respective waiting threads have
        # woken up (or are in the process of waking up) in time to pick up the
        # just-queued value, so avoid the actual operation
        # (which is much more expensive than checking)
        not_empty = self.__not_empty
        if not not_empty.isSet():
            not_empty.set()
        empty = self.__empty
        if empty.isSet():
            empty.clear()

        self.assert_started()

    @staticmethod
    def worker(self):
        self = self()
        if self is None:
            raise TerminateWorker

        task = self._dequeue()
        local = self.local
        if task is not None:
            try:
                local.working = True
                task()

                # really couldn't care less about thread safety here ;)
                self.completed_tasks += 1
                if self.completed_tasks > (1<<30):
                    self.completed_tasks = 0
            finally:
                try:
                    self._call_cleanup_callbacks()
                except:
                    self.logger.error("Error in task cleanup hook", exc_info = True)
                try:
                    del local.working
                except:
                    pass

    def in_worker(self):
        return getattr(self.local, 'working', False)

    def is_started(self):
        return not(self.__workers is None or self.__pid != os.getpid())

    def check_started(self):
        return self.is_started() and all([t.is_alive() for t in self.__workers])

    def stop(self, wait = False):
        if self.__workers:
            with self.__spawnlock:
                for w in self.__workers or ():
                    try:
                        if w.is_alive():
                            w.terminate(False)
                    except:
                        pass
                self.__workers = None

            # Wake up threads so they die awake
            self.__not_empty.set()

    def close(self):
        # Signal idle threads to commit suicide
        self.__exhausted_iter.terminate()

    def terminate(self):
        self.stop()

    def start(self):
        if not self.is_started():
            self.populate_workers()

    def assert_started(self):
        if not self.is_started():
            self.populate_workers()

    def join(self, timeout = None):
        if not self.is_started():
            return True
        if timeout is not None:
            now = time.time()
            timeout += now
        while timeout is None or now < timeout:
            if timeout is not None:
                wait_timeout = timeout - now
            elif self.__exhausted_iter._terminate:
                # If the pool is shut down in a way that aborts all queued tasks, we can't
                # merely wait for the empty event to be set, we have to also monitor suiciding threads
                if not self.is_started():
                    break
                wait_timeout = 0.1
            else:
                wait_timeout = None
            if self.__empty.wait(wait_timeout):
                # The event is not 100% certain, we can still get awakened when there's work to do
                # We have to check under __swap_lock to be sure
                with self.__swap_lock:
                    if self.__dequeue is self.__exhausted and not self.queues and not self.__workset:
                        return True
                    else:
                        # False alarm, clear it so we don't spin
                        self.__empty.clear()
            else:
                # Timeout
                return False
            if timeout is not None:
                now = time.time()
        return not self.is_started() or (self.__dequeue is self.__exhausted and not self.queues and not self.__workset)

    def populate_workers(self):
        with self.__spawnlock:
            if not self.is_started():
                name_pattern = self.name_pattern
                index_base = self.__last_worker_index
                self.__last_worker_index += self.workers
                self.__workers = [ self.Process(
                        functools.partial(self.worker, weakref.ref(self)),
                        name = name_pattern % (i+index_base,) if name_pattern is not None else None
                    ) for i in xrange(self.workers) ]
                for w in self.__workers:
                    w.logger = self.logger
                    w.daemon = True
                    w.start()

                self.__pid = os.getpid()
            # Else, just keep number of workers in sync
            elif len(self.__workers) < self.workers:
                name_pattern = self.name_pattern
                new_workers = self.workers - len(self.__workers)
                index_base = self.__last_worker_index
                self.__last_worker_index += new_workers
                nworkers = [ self.Process(
                        functools.partial(self.worker, weakref.ref(self)),
                        name = name_pattern % (i+index_base,) if name_pattern is not None else None
                    ) for i in xrange(new_workers) ]
                for w in nworkers:
                    w.daemon = True
                    w.start()
                self.__workers.extend(nworkers)
            elif len(self.__workers) > self.workers:
                nworkers = self.__workers[self.workers:]
                del self.__workers[self.workers:]
                for w in nworkers:
                    w.terminate(False)

    def apply_async(self, task, args = (), kwargs = {}, queue = None):
        if args or kwargs:
            task = functools.partial(task, *args, **kwargs)
        self._enqueue(queue, task)

    def apply(self, task, args = (), kwargs = {}, queue = None, timeout = None):
        rv = []
        ev = threading.Event()
        def stask():
            try:
                rv.append(task(*args, **kwargs))
            except:
                rv.append(ExceptionWrapper(sys.exc_info()))
            ev.set()
        self._enqueue(queue, stask)
        if ev.wait(timeout):
            rv = rv[0]
            if isinstance(rv, ExceptionWrapper):
                rv.reraise()
            else:
                return rv
        else:
            raise TimeoutError

    def subqueue(self, queue, *p, **kw):
        return SubqueueWrapperThreadPool(self, queue, *p, **kw)

class SubqueueWrapperThreadPool:
    """
    Re-implementation of multiprocessing.pool.ThreadPool optimized for threads
    and asynchronous result-less tasks.
    Implements quasi-lockless double-buffering so queue insertions are very fast, and
    multiple queues for managing fairness.

    This implementation is forcibly a daemon thread pool, which when destroyed
    will cancel all pending tasks, and no task returns any result, and has been
    optimized for that usage pattern.
    """

    def __init__(self, pool, queue, priority = None):
        self.queue = queue
        self.pool = pool
        if priority is not None:
            self.set_queueprio(priority)

    def queuelen(self):
        return self.pool.queuelen(self.queue)

    # alias for multiprocessing.pool compatibility
    qsize = queuelen

    # alias for multiprocessing.pool compatibility
    @property
    def _taskqueue(self):
        return self

    @property
    def local(self):
        return self.pool.local

    def in_worker(self):
        return self.pool.in_worker()

    def queueprio(self):
        return self.pool.queueprio(self.queue)

    def set_queueprio(self, prio):
        return self.pool.set_queueprio(prio, self.queue)

    def is_started(self):
        return self.pool.is_started()

    def check_started(self):
        return self.pool.check_started()

    def stop(self, wait = False):
        # Must stop the main pool, not the wrapper
        # Don't complain though
        pass

    def close(self):
        pass

    def terminate(self):
        pass

    def start(self):
        return self.pool.start()

    def assert_started(self):
        return self.pool.assert_started()

    def join(self, timeout = None):
        # To-do: join only the subqueue
        return self.pool.join(timeout)

    def populate_workers(self):
        return self.pool.populate_workers()

    def apply_async(self, task, args = (), kwargs = {}):
        return self.pool.apply_async(task, args, kwargs, queue = self.queue)

    def apply(self, task, args = (), kwargs = {}, timeout = None):
        return self.pool.apply(task, args, kwargs, self.queue, timeout)

