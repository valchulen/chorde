# -*- coding: utf-8 -*-
from threading import Thread
import logging

_logger = logging.getLogger("WorkerThread")

_cleanupHooks = []

class NotDoneError(Exception):
    pass

def registerThreadCleanupFunction(f):
    _cleanupHooks.append(f)

def _callCleanupHooks():
    global _logger

    for f in _cleanupHooks:
        try:
            f()
        except:
            _logger.error("Exception ocurred within thread cleanup function:", exc_info = True)

class WorkerThread(Thread):
    def __init__(self,routine,*args,**kwargs):
        Thread.__init__(self)
        self.routine = routine
        self.args = args
        self.kwargs = kwargs
        self._rv = None
        self._exc = None
        self.force_threading = False
        self.running = False

    def run(self):
        global _logger, _callCleanupHooks
        try:
            try:
                self.running = True
                self._rv = self.routine(*self.args,**self.kwargs)
            finally:
                _callCleanupHooks()
                self.running = False
        except Exception,e:
            self._exc = e
            _logger.error("Exception ocurred in worker thread:", exc_info = True)

    def start(self):
        global _nothreads
        self.running = True
        Thread.start(self)

    def rv(self, nowait=False):
        if self.running:
            if nowait:
                raise NotDoneError()
            self.join()
        try:
            if self._exc is not None:
                raise self._exc
            return self._rv
        finally:
            self._exc = None
            self._rv = None
