from .sPickle import SecurePickler, SecureUnpickler
from .clients import *
from .clients.inproc import Cache as LRUCache, CuckooCache as CuckooCache
from ._version import __version__

try:
    from .mq import *
except ImportError:
    import warnings
    warnings.warn("No messaging built in, some features may not work")
    del warnings


