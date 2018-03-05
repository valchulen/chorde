# -*- coding: utf-8 -*-
import random
import threading
import functools
import weakref
import itertools
import json
import zmq
import time
import operator
import logging

from . import ipsub

try:
    import cStringIO
except ImportError:
    import StringIO as cStringIO

from chorde.clients import CacheMissError
from chorde.clients import inproc

P2P_HWM = 10
INPROC_HWM = 1 # Just for wakeup signals
PENDING_TIMEOUT = 10.0

class OOB_UPDATE:
    pass
class CLEAR:
    pass

DEFAULT_P2P_BINDHOSTS = (
    "ipc:///tmp/coherence-%(identity)s-%(randport)s",
    "tcp://localhost"
)

class NoopWaiter(object):
    __slots__ = ()
    def __init__(self, manager = None, txid = None):
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


def _psufix(x):
    return x[:16] + x[-16:]

HASHES = {
    int : lambda x : hash(x) & 0xFFFFFFFF, # hash(x:int) = x, but must be 64/32-bit compatible
    long : lambda x : hash(x) & 0xFFFFFFFF, # must be 64/32-bit compatible
    str : _psufix, # prefix+suffix is ok for most
    unicode : lambda x : _psufix(x.encode("utf8")),
    list : len,
    set : len,
    dict : len,
    tuple : len,
    None : type,
}

def stable_hash(x):
    return HASHES.get(type(x), HASHES.get(None))(x)

def _weak_callback(f):
    @functools.wraps(f)
    def rv(wself, *p, **kw):
        self = wself()
        if self is None:
            return False
        else:
            return f(self, *p, **kw)
    return staticmethod(rv)

def _bound_weak_callback(self, f):
    return functools.partial(f, weakref.ref(self))

def _mkwaiter(ctx, socktype, prefix):
    waiter = ctx.socket(zmq.PAIR)
    for _ in xrange(5):
        waiter_id = "inproc://%s%x.%x" % (prefix, id(waiter),random.randint(0,1<<30))
        try:
            waiter.bind(waiter_id)
            break
        except:
            pass
    else:
        try:
            waiter_id = "inproc://%s%x.%x" % (prefix, id(waiter),random.randint(0,1<<30))
            waiter.bind(waiter_id)
        except:
            waiter.close()
            raise
    return waiter, waiter_id

def _swallow_connrefused(onerror):
    def decor(f):
        @functools.wraps(f)
        def rv(*p, **kw):
            try:
                return f(*p, **kw)
            except zmq.ZMQError as e:
                if e.errno in (zmq.ECONNREFUSED,):
                    return onerror(*p, **kw)
                else:
                    raise
        return rv
    return decor

def _noop(*p, **kw):
    return

class CoherenceManager(object):
    def __init__(self, namespace, private, shared, ipsub_,
            p2p_pub_bindhosts = DEFAULT_P2P_BINDHOSTS,
            encoding = 'pyobj',
            synchronous = False,
            quick_refresh = False,
            stable_hash = stable_hash,
            value_pickler = None,
            max_pending = 10240,
            logger = None):
        """
        Params
            namespace: A namespace that will use to identify events in subscription
                chatter. If granular enough, it will curb chatter considerably.

            private: A client to a private cache

            shared: (optional) A client to a shared cache. If not given,
                p2p transfer of serialized values will be required. If not given,
                a value_pickler must be given.

            ipsub_: An Inter-Process Subscription manager, already
                bound to some endpoint.

            p2p_pub_bindhosts: A list of bindhosts for the P2P pair publisher.

            encoding: The encoding to be used on messages. Default is pyobj, which
                is full-featured, but can be slow or unsafe if the default pickler
                is used (see IPSub.register_pyobj). This only applies to keys,
                values will be encoded independently with value_picker.

            synchronous: If True, fire_X events will be allowed to wait. It will
                involve a big chatter overhead, requiring N:1 confirmations to be
                routed around the IPSub, which requires 2 roundtrips but N bandwidth,
                and monitoring and transmission of heartbeats to know about all peers.
                If False, calls to fire_X().wait will be no-ops.

            quick_refresh: If True, the manager will subscribe to all "done" events,
                as to maintain private cache consistency as close as possible.

            stable_hash: If provided, it must be a callable that computes stable
                key hashes, used to subscribe to specific key pending notifications.
                If not provided, the default will be used, which can only handle
                basic types. It should be fast.

            max_pending: When running in as a designated broker, IPSub will have to
                track all pending computations. This limits how many pending keys
                are kept in-memory. It defaults to 10k. While it is rare you'll
                have 10k pending keys in a node, you might have 10k missed done
                notifications within a task timeout period, and that could eat all
                your node's RAM rather quickly. This setting avoids OOM conditions,
                by expiring old pending entries as the list reaches this limit.

            logger: Optionally override the default logger (chorde.mq.coherence)
        """
        assert value_pickler or shared

        if logger is None:
            logger = logging.getLogger('chorde.mq.coherence')
        self.logger = logger

        self.private = private
        self.shared = shared
        self.ipsub = ipsub_
        self.local = threading.local()
        self.namespace = namespace
        self.synchronous = synchronous
        self.quick_refresh = quick_refresh
        self.stable_hash = stable_hash
        self.encoding = encoding
        self._txid = itertools.cycle(xrange(0x7FFFFFFF))

        # Key -> hash
        self.pending = dict()
        self.group_pending = inproc.Cache(max_pending, False)
        self.recent_done = inproc.Cache(max_pending, False)
        self.selfdone_subs = set()
        self._last_tic_request = 0

        if synchronous:
            self.waiter = SyncWaiter
        else:
            self.waiter = NoopWaiter

        bindargs = dict(
            randport = 50000 + int(random.random() * 20000),
            identity = ipsub_.identity
        )
        self.p2p_pub_bindhosts = [ bindhost % bindargs for bindhost in p2p_pub_bindhosts ]
        self.p2p_pub_binds = [ipsub_.identity] # default contact is ipsub identity

        # Active broadcasts
        self.delprefix = namespace + '|c|del|'
        self.delackprefix = namespace + '|c|delack|'

        # Listener -> Broker requests
        self.pendprefix = namespace + '|c|pend|'
        self.pendqprefix = namespace + '|c|pendq|'
        self.doneprefix = namespace + '|c|done|'
        self.abortprefix = namespace + '|c|abrt|'

        # Broker -> Listener requests
        self.listpendqprefix = namespace + '|c|listpendq|'

        # FSM state event lister handles, not registered yet
        self.encoded_pending = None
        self.encoded_done = None
        self.encoded_abort = None
        self.encoded_pending_query = None

        self.bound_pending = _bound_weak_callback(self, self._on_pending)
        self.bound_done = _bound_weak_callback(self, self._on_done)
        self.bound_abort = _bound_weak_callback(self, self._on_abort)
        self.bound_pending_query = _bound_weak_callback(self, self._on_pending_query)
        self.bound_list_pending_query = _bound_weak_callback(self, self._on_list_pending_query)
        self.bound_deletion = _bound_weak_callback(self, self._on_deletion)
        self.bound_tic = _bound_weak_callback(self, self._on_tic)
        self.encoded_pending = self.encoded_done = self.encoded_pending_query = None

        ipsub_.listen_decode(self.delprefix, ipsub.EVENT_INCOMING_UPDATE,
            self.bound_deletion )
        ipsub_.listen('', ipsub.EVENT_ENTER_BROKER,
            _bound_weak_callback(self, self._on_enter_broker) )
        ipsub_.listen('', ipsub.EVENT_LEAVE_BROKER,
            _bound_weak_callback(self, self._on_leave_broker) )
        ipsub_.listen('', ipsub.EVENT_ENTER_LISTENER,
            _bound_weak_callback(self, self._on_enter_listener) )
        ipsub_.listen('', ipsub.EVENT_LEAVE_LISTENER,
            _bound_weak_callback(self, self._on_leave_listener) )

        if ipsub_.is_running:
            # Must manually invoke enter event
            if ipsub_.is_broker:
                self._on_enter_broker(weakref.ref(self), None, ipsub.EVENT_ENTER_BROKER, None)
            else:
                self._on_enter_listener(weakref.ref(self), None, ipsub.EVENT_ENTER_LISTENER, None)

    @property
    def txid(self):
        """
        Generate a new transaction id. No two reads will be the same... often.
        """
        # Iterator is atomic, no need for locks
        return self._txid.next()

    @_swallow_connrefused(_noop)
    def fire_deletion(self, key, timeout = None):
        txid = self.txid
        waiter = self.waiter(self, txid) # subscribe before publishing, or we'll miss it
        if key is CLEAR:
            # Cannot use any other encoding for this
            encoding = 'pyobj'
        else:
            encoding = self.encoding
        self.ipsub.publish_encode(self.delprefix, encoding, (txid, key), timeout = timeout)
        return waiter

    @_weak_callback
    def _on_deletion(self, prefix, event, payload):
        txid, key = payload
        if key is CLEAR:
            # Wowowow
            self.logger.debug('CLEAR')
            self.private.clear()
        else:
            try:
                self.private.delete(key)
            except CacheMissError:
                pass

        if self.synchronous:
            self.ipsub.publish_encode(self.delackprefix, self.encoding, txid)

        return True

    @_weak_callback
    def _on_enter_broker(self, prefix, event, payload):
        self.logger.debug('Entered broker for prefix %s', prefix)
        ipsub_ = self.ipsub
        self.encoded_pending = ipsub_.listen_decode(self.pendprefix, ipsub.EVENT_INCOMING_UPDATE,
            self.bound_pending )
        self.encoded_done = ipsub_.listen_decode(self.doneprefix, ipsub.EVENT_INCOMING_UPDATE,
            self.bound_done )
        self.encoded_abort = ipsub_.listen_decode(self.abortprefix, ipsub.EVENT_INCOMING_UPDATE,
            self.bound_abort )
        self.encoded_pending_query = ipsub_.listen_decode(self.pendqprefix, ipsub.EVENT_INCOMING_UPDATE,
            self.bound_pending_query )
        ipsub_.listen('', ipsub.EVENT_TIC, self.bound_tic)
        ipsub_.publish_encode(self.listpendqprefix, self.encoding, None)
        return True

    @_weak_callback
    def _on_leave_broker(self, prefix, event, payload):
        self.logger.debug('Left broker for prefix %s', prefix)
        ipsub_ = self.ipsub
        ipsub_.unlisten(self.pendprefix, ipsub.EVENT_INCOMING_UPDATE,
            self.encoded_pending )
        ipsub_.unlisten(self.doneprefix, ipsub.EVENT_INCOMING_UPDATE,
            self.encoded_done )
        ipsub_.unlisten(self.abortprefix, ipsub.EVENT_INCOMING_UPDATE,
            self.encoded_abort )
        ipsub_.unlisten(self.pendqprefix, ipsub.EVENT_INCOMING_UPDATE,
            self.encoded_pending_query )
        ipsub_.unlisten('', ipsub.EVENT_TIC, self.bound_tic)
        return True

    @_weak_callback
    def _on_enter_listener(self, prefix, event, payload):
        self.logger.debug('Entered listener for prefix %s', prefix)
        ipsub_ = self.ipsub
        ipsub_.listen(self.listpendqprefix, ipsub.EVENT_INCOMING_UPDATE,
            self.bound_list_pending_query )
        if self.quick_refresh:
            self.encoded_done = ipsub_.listen_decode(self.doneprefix, ipsub.EVENT_INCOMING_UPDATE,
                self.bound_done )
            self.encoded_abort = ipsub_.listen_decode(self.abortprefix, ipsub.EVENT_INCOMING_UPDATE,
                self.bound_abort )
        return True

    @_weak_callback
    def _on_leave_listener(self, prefix, event, payload):
        self.logger.debug('Left listener for prefix %s', prefix)
        ipsub_ = self.ipsub
        ipsub_.unlisten(self.listpendqprefix, ipsub.EVENT_INCOMING_UPDATE,
            self.bound_list_pending_query )
        if self.quick_refresh:
            ipsub_.unlisten(self.doneprefix, ipsub.EVENT_INCOMING_UPDATE,
                self.encoded_done )
            ipsub_.unlisten(self.abortprefix, ipsub.EVENT_INCOMING_UPDATE,
                self.encoded_abort )
        return True

    @_weak_callback
    def _on_tic(self, prefix, event, payload):
        # Bye recents, no longer recent
        self.recent_done.clear()

        if not self.ipsub.is_broker:
            return False

        # Check pending freshness, ask for refreshers
        now = time.time()
        needs_refresh = False
        _PENDING_TIMEOUT = PENDING_TIMEOUT
        _REFRESH_TIMEOUT = PENDING_TIMEOUT/2
        group_pending = self.group_pending
        try:
            # Try iterating
            for rv in group_pending.itervalues():
                if (now - rv[1]) > _REFRESH_TIMEOUT:
                    needs_refresh = True
                    break
        except RuntimeError:
            # Bah, gotta snapshot
            for rv in group_pending.values():
                if (now - rv[1]) > _REFRESH_TIMEOUT:
                    needs_refresh = True
                    break

        if needs_refresh:
            # First, clean up really expired values
            clean = []
            needs_refresh = False
            try:
                # Try iterating
                for k,rv in group_pending.iteritems():
                    delta = now - rv[1]
                    if delta > _PENDING_TIMEOUT:
                        clean.append(k)
                    elif delta > _REFRESH_TIMEOUT:
                        needs_refresh = True
            except RuntimeError:
                # Bah, gotta snapshot
                del clean[:]
                for k,rv in group_pending.items():
                    delta = now - rv[1]
                    if delta > _PENDING_TIMEOUT:
                        clean.append(k)
                    elif delta > _REFRESH_TIMEOUT:
                        needs_refresh = True

            # Expire them
            try:
                pop_pending = group_pending.pop
                for k in clean:
                    pop_pending(k, None)
                if len(clean) > 0:
                    self.logger.debug("Cleaned %d expired pending items", len(clean))
            except KeyError:
                pass

            if needs_refresh:
                # Ask for a refreshment
                self.logger.debug("Requesting fresh pending items list")
                self.ipsub.publish_encode(self.listpendqprefix, self.encoding, None)

        return True

    def _request_tic(self, now = None):
        if now is None:
            now = time.time()
        if (now - self._last_tic_request) > (PENDING_TIMEOUT/2):
            self._last_tic_request = now
            self.ipsub.request_tic()
            return True
        else:
            return False

    def _query_pending_locally(self, key, expired, timeout = 2000, optimistic_lock = False):
        rv = self.group_pending.get(key)
        now = time.time()
        if rv is not None and (now - rv[1]) > PENDING_TIMEOUT:
            rv = None
        elif rv is not None and (now - rv[1]) > (PENDING_TIMEOUT/2):
            # Um... belated tick?
            if self._request_tic(now):
                self.logger.debug("Requesting async fresh pending items list")
        if rv is not None:
            return rv[-1]
        else:
            rv = self.pending.get(key)
            if rv is not None:
                return self.p2p_pub_binds
            elif expired():
                if optimistic_lock:
                    now = time.time()
                    txid = self.txid
                    self.recent_done.pop(key,None)
                    self.group_pending[key] = (txid, now, self.p2p_pub_binds)
                    self.pending[key] = txid
                return None
            else:
                return OOB_UPDATE
        return rv

    @_swallow_connrefused(_query_pending_locally)
    def query_pending(self, key, expired, timeout = 2000, optimistic_lock = False):
        """
        Queries the cluster about the key's pending status,
        returns contact info of current computation node if any,
        or OOB_UPDATE if an out-of-band update was detected.

        Params
            key: The key to query about
            expired: A callable that reconfirms expired status,
                to detect out-of-band updates.
            timeout: A timeout, if no replies were received by this time, it will
                be assumed no node is computing.
            optimistic_lock: Request the broker to mark the key as pending if
                there is noone currently computing, atomically.
        """
        if self.ipsub.is_broker or not self.ipsub.is_running:
            # Easy peachy
            return self._query_pending_locally(key, expired, timeout, optimistic_lock)

        # Listener... a tad more complex
        ipsub_ = self.ipsub
        txid = self.txid if optimistic_lock else None
        req = ipsub_.encode_payload(self.encoding, (
            key,
            txid,
            self.p2p_pub_binds,
            optimistic_lock))

        ctx = ipsub_.context
        waiter, waiter_id = _mkwaiter(ctx, zmq.PAIR, "qpw")
        try:
            def signaler(prefix, event, message, req = req):
                if message[0][2:] == req:
                    # This is our message
                    signaler = ctx.socket(zmq.PAIR)
                    signaler.connect(waiter_id)
                    signaler.send(message[1][-1])
                    signaler.close()
                    return False
                else:
                    return True
            ipsub_.listen(self.namespace, ipsub.EVENT_UPDATE_ACKNOWLEDGED, signaler)
            ipsub_.publish(self.pendqprefix, req)
            for i in xrange(3):
                if waiter.poll(timeout/4):
                    break
                elif expired():
                    ipsub_.publish(self.pendqprefix, req)
                else:
                    break
            else:
                waiter.poll(timeout/4)
            if waiter.poll(1):
                try:
                    rv = json.loads(waiter.recv())
                except ValueError:
                    # It happens here that the IPSub returns its OK reply, when
                    # there are no registered brokers on our namespace. Means it's up to us.
                    # Enter broker mode and answer our caller locally
                    if ipsub_.is_running:
                        if ipsub_.is_broker:
                            self._on_leave_listener(weakref.ref(self), None, ipsub.EVENT_LEAVE_LISTENER, None)
                            self._on_enter_broker(weakref.ref(self), None, ipsub.EVENT_ENTER_BROKER, None)
                        else:
                            self._on_leave_broker(weakref.ref(self), None, ipsub.EVENT_LEAVE_BROKER, None)
                            self._on_enter_listener(weakref.ref(self), None, ipsub.EVENT_ENTER_LISTENER, None)
                    return self._query_pending_locally(key, expired, timeout, optimistic_lock)
                if rv is not None:
                    rv = rv[-1]
                elif not expired():
                    rv = OOB_UPDATE
            elif expired():
                rv = None
            else:
                rv = OOB_UPDATE
            ipsub_.unlisten(self.namespace, ipsub.EVENT_UPDATE_ACKNOWLEDGED, signaler)
        finally:
            waiter.close()

        if optimistic_lock and rv is None:
            # We acquired it
            self.recent_done.pop(key,None)
            self.pending[key] = txid
        return rv

    def _publish_pending(self, keys):
        # Publish pending notification for anyone listening
        payload = (
            # pending data
            self.txid,
            keys,
            # contact info data
            self.p2p_pub_binds,
        )
        self.ipsub.publish_encode(self.pendprefix, self.encoding, payload)

    @_weak_callback
    def _on_pending_query(self, prefix, event, payload):
        key, txid, contact, lock = payload
        rv = self.group_pending.get(key)
        now = time.time()
        if rv is not None and (now - rv[1]) > PENDING_TIMEOUT:
            # Expired
            rv = None
        elif rv is not None and (now - rv[1]) > (PENDING_TIMEOUT/2):
            # Um... belated tick?
            if self._request_tic(now):
                self.logger.debug("Requesting async fresh pending items list")
        if rv is None:
            # Maybe the broker itself is computing...
            rv = self.pending.get(key)
            if rv is not None:
                rv = (rv, now, self.p2p_pub_binds)
        if lock and rv is None:
            self.group_pending[key] = (txid, now, contact)
        return ipsub.BrokerReply(json.dumps(rv))

    @_weak_callback
    def _on_list_pending_query(self, prefix, event, payload):
        if not self.ipsub.is_broker:
            if self.pending:
                self._publish_pending(list(self.pending))
            return True

    @_weak_callback
    def _on_pending(self, prefix, event, payload):
        if self.ipsub.is_broker:
            txid, keys, contact = payload
            self.group_pending.update(itertools.izip(
                keys, itertools.repeat((txid,time.time(),contact),len(keys))))
            return True

    @_weak_callback
    def _on_done(self, prefix, event, payload):
        if self.ipsub.is_broker:
            txid, keys, contact = payload
            group_pending = self.group_pending
            ctxid = (txid, contact)
            proj = operator.itemgetter(0,-1)
            pnone = (None, None)
            private = self.private
            for key in keys:
                if proj(group_pending.get(key, pnone)) == ctxid:
                    try:
                        del group_pending[key]
                    except KeyError:
                        pass
                    if private.contains(key):
                        try:
                            self.private.expire(key)
                        except CacheMissError:
                            pass
        else:
            # Just deletion-like
            txid, keys, contact = payload
            private = self.private
            for key in keys:
                if private.contains(key):
                    try:
                        self.private.expire(key)
                    except CacheMissError:
                        pass
        return True

    @_weak_callback
    def _on_abort(self, prefix, event, payload):
        if self.ipsub.is_broker:
            txid, keys, contact = payload
            group_pending = self.group_pending
            ctxid = (txid, contact)
            proj = operator.itemgetter(0,-1)
            pnone = (None, None)
            for key in keys:
                if proj(group_pending.get(key, pnone)) == ctxid:
                    try:
                        del group_pending[key]
                    except KeyError:
                        pass
        return True

    def _fire_selfdone(self, key):
        rem = set()
        for sub in set(self.selfdone_subs):
            if not sub(key):
                rem.add(sub)
        if rem:
            self.selfdone_subs -= rem

    def _sub_selfdone(self, sub):
        self.selfdone_subs.add(sub)

    def _unsub_selfdone(self, sub):
        try:
            self.selfdone_subs.remove(sub)
        except KeyError:
            pass

    def mark_done(self, key, timeout = None):
        txid = self.pending.pop(key, self.group_pending.pop(key, (None,))[0])
        self.recent_done[key] = time.time()
        if txid is not None:
            self.fire_done([key], txid, timeout = timeout)
        self._fire_selfdone(key)

    def mark_aborted(self, key, timeout = None):
        txid = self.pending.pop(key, self.group_pending.pop(key, (None,))[0])
        self.recent_done[key] = time.time()
        if txid is not None:
            self.fire_aborted([key], txid, timeout = timeout)
        self._fire_selfdone(key)

    @_swallow_connrefused(_noop)
    def fire_done(self, keys, txid = None, timeout = None):
        if keys:
            if txid is None:
                txid = self.txid
            first_key = iter(keys).next()
            self.ipsub.publish_encode(self.doneprefix+str(self.stable_hash(first_key)), self.encoding,
                (txid, keys, self.p2p_pub_binds),
                timeout = timeout)
        return NoopWaiter()

    @_swallow_connrefused(_noop)
    def fire_aborted(self, keys, txid = None, timeout = None):
        if keys:
            if txid is None:
                txid = self.txid
            first_key = iter(keys).next()
            self.ipsub.publish_encode(self.abortprefix+str(self.stable_hash(first_key)), self.encoding,
                (txid, keys, self.p2p_pub_binds),
                timeout = timeout)
        return NoopWaiter()

    @_swallow_connrefused(_noop)
    def wait_done(self, key, poll_interval = 1000, timeout = None):
        """
        Waits until the given key is removed from pending state.

        If the entry is still expired after this function returns,
        it's sensible to assume that the computation node has aborted,
        and an optimistic lock should be re-attempted.

        Returns
            True if the task is done, False otherwise. True always
            if there's no timeout.

        Params
            key: The key to wait for
            poll_interval: A timeout(ms), pending status
                rechecks will be performed in this interval.
            timeout: maximum time to wait (in ms). If not None,
                the method's return value must be checked for success.
        """

        # Check for recent notifications
        recent = self.recent_done.get(key)
        if recent is not None and (time.time() - recent) < (poll_interval * 1.01):
            # don't wait then
            return True

        ipsub_ = self.ipsub
        keysuffix = str(self.stable_hash(key))
        doneprefix = self.doneprefix+keysuffix
        abortprefix = self.abortprefix+keysuffix

        ctx = ipsub_.context
        waiter, waiter_id = _mkwaiter(ctx, zmq.PAIR, "qpw")
        dsignaler = asignaler = ssignaler = None
        try:
            contact_cell = []
            def signaler(prefix, event, payload):
                txid, keys, contact = payload
                if key in keys:
                    # This is our message
                    contact_cell[:] = contact
                    signaler = ctx.socket(zmq.PAIR)
                    signaler.connect(waiter_id)
                    signaler.send("")
                    signaler.close()
                    return False
                else:
                    return True
            dsignaler = ipsub_.listen_decode(doneprefix, ipsub.EVENT_INCOMING_UPDATE, signaler)
            asignaler = ipsub_.listen_decode(abortprefix, ipsub.EVENT_INCOMING_UPDATE, signaler)
            ssignaler = lambda key, contact = self.p2p_pub_binds : signaler(None, None, (None, [key], contact))
            self._sub_selfdone(ssignaler)
            success = False
            while timeout is None or timeout > 0:
                # Check for recent self-notifications
                recent = self.recent_done.get(key)
                if recent is not None and (time.time() - recent) < (poll_interval * 1.01):
                    # don't wait then
                    success = True
                    break
                if waiter.poll(min(poll_interval, timeout or poll_interval)):
                    success = True
                    break
                elif timeout is not None:
                    timeout -= poll_interval
                # Re-check for recent self-notifications
                recent = self.recent_done.get(key)
                if recent is not None and (time.time() - recent) < (poll_interval * 1.01):
                    # don't wait then
                    success = True
                    break
                # Request confirmation of pending status
                if not self.query_pending(key, lambda:1, poll_interval, False):
                    success = True
                    break
        finally:
            if ssignaler is not None:
                self._unsub_selfdone(ssignaler)
            if dsignaler is not None:
                ipsub_.unlisten(doneprefix, ipsub.EVENT_INCOMING_UPDATE, dsignaler)
            if asignaler is not None:
                ipsub_.unlisten(abortprefix, ipsub.EVENT_INCOMING_UPDATE, asignaler)
            waiter.close()
        return success
