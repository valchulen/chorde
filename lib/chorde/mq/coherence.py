# -*- coding: utf-8 -*-
import random
import threading
import functools
import weakref
import itertools

from . import ipsub

from chorde.clients import CacheMissError

P2P_HWM = 10
INPROC_HWM = 1 # Just for wakeup signals

DEFAULT_P2P_BINDHOSTS = (
    "ipc:///tmp/coherence-%(identity)s-%(randport)s", 
    "tcp://localhost"
)

class NoopWaiter(object):
    __slots__ = ()
    def __init__(self, manager, txid):
        pass
    def wait(self):
        pass

class SyncWaiter(object):
    __slots__ = ('manager','txid')
    def __init__(self, manager, txid):
        self.manager = manager
        self.txid = txid
        manager.listen_decode(manager.ackprefix, ipsub.EVENT_INCOMING_UPDATE, self)
    def wait(self):
        raise NotImplementedError
    def __call__(self, prefix, event, payload):
        return False
    def __del__(self):
        manager = self.manager
        manager.unlisten(manager.ackprefix, ipsub.EVENT_INCOMING_UPDATE, self)

class CoherenceManager(object):
    def __init__(self, namespace, private, shared, ipsub, 
            p2p_pub_bindhosts = DEFAULT_P2P_BINDHOSTS, 
            encoding = 'pyobj',
            synchronous = False):
        """
        Params
            namespace: A namespace that will use to identify events in subscription
                chatter. If granular enough, it will curb chatter considerably.
            
            private: A client to a private cache

            shared: (optional) A client to a shared cache. If not given,
                p2p transfer of serialized values will be required.

            ipsub: An Inter-Process Subscription manager, already
                bound to some endpoint.

            p2p_pub_bindhosts: A list of bindhosts for the P2P pair publisher.

            encoding: The encoding to be used on messages. Default is pyobj, which
                is full-featured, but can be slow or unsafe if the default pickler
                is used (see IPSub.register_pyobj).

            synchronous: If True, fire_X events will be allowed to wait. It will
                involve a big chatter overhead, requiring N:1 confirmations to be
                routed around the IPSub, which requires 2 roundtrips but N bandwidth,
                and monitoring and transmission of heartbeats to know about all peers.
                If False, calls to fire_X().wait will be no-ops.
        """
        self.private = private
        self.shared = shared
        self.ipsub = ipsub
        self.local = threading.local()
        self.namespace = namespace
        self.synchronous = synchronous
        self._txid = itertools.cycle(xrange(0x7FFFFFFF))

        if synchronous:
            self.waiter = SyncWaiter
        else:
            self.waiter = NoopWaiter

        bindargs = dict(
            randport = 50000 + int(random.random() * 20000),
            identity = ipsub.identity
        )
        self.p2p_pub_bindhosts = [ bindhost % bindargs for bindhost in p2p_pub_bindhosts ]

        self.delprefix = namespace + '|coh|del|'
        self.delackprefix = namespace + '|coh|delack|'

        ipsub.listen_decode(self.delprefix, ipsub.EVENT_INCOMING_UPDATE, 
            functools.partial(self._on_deletion, weakref.ref(self)) )

    @property
    def txid(self):
        """
        Generate a new transaction id. No two reads will be the same... often.
        """
        return self._txid.next()

    def fire_deletion(self, key):
        txid = self.txid
        waiter = self.waiter(self, txid) # subscribe before publishing, or we'll miss it
        self.ipsub.publish_encode(self.delprefix, self.encoding, (txid, key))
        return waiter

    @staticmethod
    def _on_deletion(wself, prefix, event, payload):
        self = wself()
        if self is None:
            return

        key, txid = payload
        try:
            self.private.delete(key)
        except CacheMissError:
            pass
        
        if self.synchronous:
            self.ipsub.publish_encode(self.delackprefix, self.encoding, txid)


