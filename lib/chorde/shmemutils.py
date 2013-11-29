# -*- coding: utf-8 -*-
import os
import os.path

import ctypes
import mmap

import random
import tempfile
import threading

try:
    import fcntl
except ImportError:
    # Platform sucks
    fcntl = None # lint:ok

try:
    import numpy
except ImportError:
    try:
        # PyPy ?
        import numpypy as numpy  # lint:ok
    except ImportError:
        # Ouch
        numpy = None  # lint:ok

class SharedCounterGenericBase(object):
    """
    Maps size() bytes from the buffer into a shared counters
    structure with process-local subcounters. Reading the
    counters takes O(slots) time, updating takes 
    O(1) and no operation blocks on other processes.
    Reads are only guaranteed to be eventually consistent,
    no snapshot semantics are guaranteed.

    If locked is given and True, slots must only be big
    enough to hold all concurrent process subcounters,
    and it is assumed the whole mapping operation will
    be free from concurrent mapping operations.

    If locked isn't given or is False, only a pid-derived
    counter can be used by this process, and if the slot
    has been used by another process, an AssertionError
    will be raised.

    SharedCounterGenericBase objects and derivations
    are both thread and process-safe, as long as you don't
    map a file/buffer more than once through different instances.
    """
    # Pick one
    dtype = None
    btype = None
    slots_item_size = None # make sizeof(dtype)
    bitmap_item_size = None # make sizeof(btype)

    def __init__(self, slots, bitmap, counters, locked):
        slot = os.getpid() % slots

        # Try to acquire the slot
        if bitmap[slot]:
            if not locked:
                raise AssertionError, "Slot occupied"
            else:
                # With a locked bitmap, we can search other slots
                for offs in xrange(slots):
                    nslot = (slot+offs) % slots
                    if not bitmap[nslot]:
                        slot = nslot
                else:
                    raise AssertionError, "All slots occupied"

        bitmap[slot] = True
        self.bitmap = bitmap
        self.slots = counters
        self.slot = slot
        self.basemap = None
        self.baseoffset = None
        self.timestamp = None
        self.cached_timestamp = None
        self.cached_value = None
        self.__rnd = random.getrandbits(62) + self.slot
        self.__wlock = threading.Lock()

    @classmethod
    def size(cls, slots):
        return (cls.slots_item_size + cls.bitmap_item_size) * slots + ctypes.sizeof(ctypes.c_uint64())

    def _update_ts(self):
        # Careful to maintain out-of-sync status out-of-sync
        ts = self.timestamp
        rand = self.__rnd
        if self.cached_timestamp is not None:
            self.cached_timestamp = (self.cached_timestamp + rand) & 0xffffffffffffffff
        ts.value += rand

    @property
    def value(self):
        if self.cached_value is None or self.cached_timestamp != self.timestamp.value:
            self.cached_timestamp = self.timestamp.value
            self.cached_value = self._value
            self.__rnd = random.getrandbits(62) + self.slot
        return self.cached_value

    def flush(self):
        if self.basemap is not None:
            self.basemap.flush()

    @property
    def _value(self):
        # Really, provide a better implementation
        return sum(self.slots)

    @classmethod
    def from_path(cls, slots, pathname, offset = 0):
        """
        Maps the contents of fileo at offset "offset" interpreting
        them as shared counters of "slots" slots, locking the file
        for exclusive access during the duration of slot acquisition
        (which is quick) if a suitable locking implementation is present.

        If the file does not exist, it creates them in a manner that
        avoids race conditions, and initializes the counter to 0.

        Raises AssertionError if it cannot aquire the slot.
        """
        if not os.path.exists(pathname) or not os.access(pathname, os.R_OK | os.W_OK):
            # Initialize
            #
            # To make it portable, and atomic on all platforms with any filesystem,
            # we create a temporary unique file and atomically put it into our destination.
            #
            # We must make sure the temporary is on the same location as the destination,
            # to make sure the rename is quick and atomic.
            #
            # On unix-like systems, rename overwrites silently, so we use a link instead,
            # which doesn't. On other systems, we use rename.
            #
            # In any case, if the rename fails, we assume someone won over us, and open
            # the existing file instead.
            try:
                tmpfileno, tmppath = tempfile.mkstemp(
                    prefix = os.path.basename(pathname),
                    dir = os.path.dirname(pathname) )
                try:
                    # Fill with zeros
                    size = cls.size(slots)
                    zeros = '\x00' * 1024
                    while size >= 1024:
                        os.write(tmpfileno, zeros)
                        size -= 1024
                    if size:
                        os.write(tmpfileno, buffer(zeros,0,size))

                    # And swap
                    if hasattr(os, 'link'):
                        os.link(tmppath, pathname)
                        os.unlink(tmppath)
                    else:
                        os.rename(tmppath, pathname)
                    return cls.from_fileno(slots, tmpfileno, offset)
                finally:
                    os.close(tmpfileno)
            except OSError:
                # Let the fallback below try
                pass
        
        # We have a locking implementation, so it's as easy as just opening
        # the file and using it. The locks will take care of only using
        # an initialized counter file
        fileno = os.open(pathname, os.O_RDWR)
        try:
            return cls.from_fileno(slots, fileno, offset)
        finally:
            os.close(fileno)

    @classmethod
    def from_file(cls, slots, fileobj, offset = 0):
        """
        Maps the contents of fileobj at offset "offset" interpreting
        them as shared counters of "slots" slots, locking the file
        for exclusive access during the duration of slot acquisition
        (which is quick) if a suitable locking implementation is present.

        Raises AssertionError if it cannot aquire the slot.
        """
        return cls.from_fileno(slots, fileobj.fileno(), offset)

    @classmethod
    def from_fileno(cls, slots, fileno, offset = 0):
        """
        Maps the contents of the given file descriptor at offset "offset" 
        interpreting them as shared counters of "slots" slots, locking the 
        file for exclusive access during the duration of slot acquisition
        (which is quick) if a suitable locking implementation is present.

        Raises AssertionError if it cannot aquire the slot.
        """
        base_offset = offset - (offset % mmap.ALLOCATIONGRANULARITY)
        remnant_offset = offset - base_offset
        size = cls.size(slots)

        buf = mmap.mmap(fileno, size, offset = base_offset)

        rv = cls.from_buffer(slots, buf, remnant_offset, fileno, offset)
        rv.basemap = buf
        rv.baseoffset = offset
        return rv

    @classmethod
    def from_buffer(cls, slots, buf, offset = 0, fileno = None, fileoffs = None):
        """
        Maps the contents of buf interpreting
        it as shared counters of "slots" slots, locking the file descriptor
        given by fileno if not None, for exclusive access during the duration 
        of slot acquisition (which is quick) if a suitable locking 
        implementation is present.

        If given, fileoffs is the start of the region to be locked
        in fileno.

        Raises AssertionError if it cannot aquire the slot.
        """
        locked = fcntl is not None and fileno is not None
        size = cls.size(slots)
        try:
            if locked:
                fcntl.lockf(fileno, fcntl.LOCK_EX, size, fileoffs)
            return cls(slots, buf, offset, locked)
        finally:
            if locked:
                fcntl.lockf(fileno, fcntl.LOCK_UN, size, fileoffs)

    def __int__(self):
        return int(self.value)
    
    def __long__(self):
        return long(self.value)

    def __float__(self):
        return float(self.value)

    def __iadd__(self, val):
        with self.__wlock:
            self.myslot.value += val
            if self.cached_value is not None:
                self.cached_value += val
            self._update_ts()
        return self

    def __isub__(self, val):
        with self.__wlock:
            self.myslot.value -= val
            if self.cached_value is not None:
                self.cached_value -= val
            self._update_ts()
        return self

    def close(self):
        # Release slot
        self.bitmap[self.slot] = False

        # Release possibly fd-holding resources
        if self.basemap is not None:
            self.basemap.flush()
            self.basemap.close()
        del self.bitmap
        del self.slots
        del self.basemap

    def __del__(self):
        self.close()

if numpy is not None:
    # Numpy-accelerated shared objects
    
    class SharedCounterBase(SharedCounterGenericBase):
        btype = numpy.bool8
        bitmap_item_size = btype().itemsize

        def __init__(self, slots, buf, offset = 0, locked = False):
            assert ctypes.sizeof(ctypes.c_bool()) == self.bitmap_item_size
            assert ctypes.sizeof(self.cdtype()) == self.slots_item_size

            timestamp = ctypes.c_uint64.from_buffer(buf, offset)
            offset += ctypes.sizeof(timestamp)
            
            # Slow, read-write bitmap
            bitmap = (ctypes.c_bool * slots).from_buffer(buf, offset)

            # Fast, read-only counters
            counters = numpy.frombuffer(buf, self.dtype, slots, 
                offset + self.bitmap_item_size * slots)
            super(SharedCounterBase, self).__init__(slots, bitmap, counters, locked)

            # Fast read-only bitmap ]:-]
            self.bitmap = numpy.frombuffer(buf, numpy.bool8, slots, offset)

            # Map my slot as a single item, it makes += atomic
            self.myslot = self.cdtype.from_buffer(buf, 
                offset + ctypes.sizeof(bitmap) + self.slots_item_size * self.slot)

            # Map timestamp
            self.timestamp = timestamp

        @property
        def _value(self):
            return self.slots.sum()

    class SharedCounter32(SharedCounterBase):
        dtype = numpy.int32
        cdtype = ctypes.c_int32
        slots_item_size = dtype().itemsize
    class SharedCounter64(SharedCounterBase):
        dtype = numpy.int64
        cdtype = ctypes.c_int64
        slots_item_size = dtype().itemsize

else:
    # Slow, but portable shared objects (based on ctypes)

    class SharedCounterBase(SharedCounterGenericBase):  # lint:ok
        btype = ctypes.c_bool
        bitmap_item_size = ctypes.sizeof(btype())
        
        def __init__(self, slots, buf, offset = 0, locked = False):
            timestamp = ctypes.c_uint64.from_buffer(buf, offset)
            offset += ctypes.sizeof(timestamp)
            
            bitmap = (ctypes.c_bool * slots).from_buffer(buf, offset)
            counters = (self.dtype * slots).from_buffer(buf, offset + ctypes.sizeof(bitmap))
            super(SharedCounterBase, self).__init__(slots, bitmap, counters, locked)

            # Map my slot as a single item, it makes += atomic
            self.myslot = self.dtype.from_buffer(buf, 
                offset + ctypes.sizeof(bitmap) + self.slots_item_size * self.slot)

            # Map timestamp
            self.timestamp = timestamp

    class SharedCounter32(SharedCounterBase):
        dtype = ctypes.c_int32
        slots_item_size = ctypes.sizeof(dtype())
    class SharedCounter64(SharedCounterBase):
        dtype = ctypes.c_int64
        slots_item_size = ctypes.sizeof(dtype())

