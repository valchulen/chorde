# -*- coding: utf-8 -*-
import weakref
import time
import zmq
import logging
import Queue
import threading
import thread
import json
import os
import random
from collections import defaultdict
from abc import ABCMeta, abstractmethod

try:
    import cPickle
except ImportError:
    import pickle as cPickle

try:
    import cStringIO
except ImportError:
    import StringIO as cStringIO

from .compat import fbuffer, bbytes

FRAME_HEARTBEAT = "__HeyDude__"
FRAME_UPDATE_OK = "OK"
FRAME_UPDATE_DROPPED = "DROP"
FRAME_VALID_UPDATE_REPLIES = (FRAME_UPDATE_OK, FRAME_UPDATE_DROPPED)

# EVENT KEY                         message payload format
EVENT_INCOMING_UPDATE = 1         # [prefix, identity, payload]. Payload verbatim as sent
EVENT_UPDATE_ACKNOWLEDGED = 2     # (update, reply frames)
EVENT_UPDATE_IGNORED = 3          # (update, reply frames)
EVENT_UPDATE_SENT = 4             # [prefix, identity, payload]. Payload verbatim as sent
EVENT_ENTER_BROKER = 5            # None
EVENT_LEAVE_BROKER = 6            # None
EVENT_ENTER_LISTENER = 7          # None
EVENT_LEAVE_LISTENER = 8          # None
EVENT_IDLE = 9                    # None
EVENT_TIC = 10                    # None

EVENT_NAMES = {
    EVENT_INCOMING_UPDATE : 'INCOMING_UPDATE',
    EVENT_UPDATE_ACKNOWLEDGED : 'UPDATE_ACKNOWLEDGED',
    EVENT_UPDATE_IGNORED: 'UPDATE_DROPPED',
    EVENT_UPDATE_SENT : 'UPDATE_SENT',
    EVENT_ENTER_BROKER : 'ENTER_BROKER',
    EVENT_LEAVE_BROKER : 'LEAVE_BROKER',
    EVENT_ENTER_LISTENER : 'ENTER_LISTENER',
    EVENT_LEAVE_LISTENER : 'LEAVE_LISTENER',
    EVENT_IDLE : 'IDLE',
    EVENT_TIC : 'TIC',
}

IDENTITY_EVENTS = (EVENT_INCOMING_UPDATE,)

BROKER_PUB_HWM = 1000
BROKER_REP_HWM = 1000
LISTENER_REQ_HWM = 1
LISTENER_SUB_HWM = 1000

INPROC_HWM = BROKER_PUB_HWM * 2

MIN_UPDATE_REPLY_FRAMES = 1
MAX_UPDATE_REPLY_FRAMES = 1
MAX_UPDATE_REPLY_FIRSTFRAME = 10
MAX_PREFIX = 256

IDLE_PERIOD = 0.5
TIC_PERIOD = 1.0

EVENT_FOR_REPLY = {
    FRAME_UPDATE_OK : EVENT_UPDATE_ACKNOWLEDGED,
    FRAME_UPDATE_DROPPED : EVENT_UPDATE_IGNORED,
}

ENCODINGS = {
    'json' : lambda x : json.dumps(x, separators=(',',':')),
    'bytes' : lambda x : x,
    'utf8' : lambda x : x.encode('utf8') if isinstance(x, unicode) else x,
}

DECODINGS = {
    'json' : json.loads,
    'bytes' : lambda x : x,
    'utf8' : lambda x : x.decode('utf8'),
}

STREAMDECODINGS = {
    'json' : json.load,
    'bytes' : lambda x : x.read(),
    'utf8' : lambda x : x.read().decode('utf8'),
}

if hasattr(zmq, 'HWM'):
    # Has single HWM
    def set_hwm(sock, hwm):
        sock.hwm = hwm
else:
    # Split HWM
    def set_hwm(sock, hwm):
        sock.set_hwm(hwm)

class BootstrapNow(Exception):
    pass

class BrokerReply(object):
    __slots__ = ('reply',)
    def __init__(self, *reply_parts):
        self.reply = reply_parts

class IPSub(object):
    class FSM(object):
        class State(object):
            __metaclass__ = ABCMeta

            def __init__(self, owner, logger = None):
                self._owner = weakref.ref(owner)
                self.logger = getattr(owner, 'logger', logging.getLogger('chorde.ipsub')) if logger is None else logger

            def transition(self, newstate):
                self.logger.debug("IPSub.FSM: LEAVE %s", self.__class__.__name__)
                self.leave()
                self.__class__ = newstate
                self.logger.debug("IPSub.FSM: ENTER %s", self.__class__.__name__)
                self.enter()

            @abstractmethod
            def enter(self):
                pass

            @abstractmethod
            def leave(self):
                pass

            @abstractmethod
            def stay(self):
                pass
        
        class Bootstrap(State):
            def enter(self):
                owner = self._owner()

                # Initialize pull socket so others can connect rightaway
                owner._pullsocket()
                
                # Try a few times
                for i in xrange(5):
                    try:
                        owner._bind()
                        break
                    except zmq.ZMQError as e:
                        if e.errno in (zmq.EADDRINUSE, zmq.ENODEV):
                            # Not a transient error, shortcut to listener
                            return self.transition(IPSub.FSM.Listener)
                    except Exception as e:
                        self.logger.info("Got %s connecting", e)
                        self.logger.debug("Got %s connecting", e, exc_info = True)
                        time.sleep(0.2)
                else:
                    try:
                        owner._bind()
                    except:
                        return self.transition(IPSub.FSM.Listener)
                self.transition(IPSub.FSM.DesignatedBroker)

            def stay(self):
                pass

            def leave(self):
                pass
        
        class Listener(State):
            def enter(self):
                owner = self._owner()
                self.listener_req = listener_req = owner._connect()
                self.listener_sub = listener_sub = owner._subscribe()
                self.pull = pull = owner._pullsocket()
                self.poller = poller = zmq.Poller()
                poller.register(listener_sub, zmq.POLLIN)
                poller.register(pull, zmq.POLLIN)
                poller.register(listener_req, zmq.POLLIN | zmq.POLLOUT)
                owner._notify_all(EVENT_ENTER_LISTENER, None)

            def stay(self):
                owner = self._owner()
                
                # Cache globals and attributes, to avoid memory allocations in the hottest loop of all
                listener_req = self.listener_req
                listener_sub = self.listener_sub
                pull = self.pull
                poller = self.poller
                POLLOUT = zmq.POLLOUT
                POLLIN = zmq.POLLIN
                HEARTBEAT_ = FRAME_HEARTBEAT
                F = False
                poller_poll = poller.poll
                poller_register = poller.register
                pull_recv_multipart = pull.recv_multipart
                no_updates = owner.updates.empty
                put_nowait = owner.updates.put_nowait
                send_update = owner._send_update
                recv_update = owner._recv_update
                recv_update_reply = owner._recv_update_reply
                int_ = int
                len_ = len
                random_ = random.random
                tic_count = 100
                buffer_ = buffer
                fbuffer_ = fbuffer
                hb_period_base = owner.heartbeat_avg_period * 2
                hb_period_spread = hb_period_base * 2
                hb_timeout = owner.heartbeat_push_timeout

                # Poll sockets
                try:
                    while not owner.stop:
                        if owner._needs_subscriptions:
                            owner._subscribe()
                        if not no_updates():
                            poller_register(listener_req, POLLIN|POLLOUT)
                        activity = poller_poll(hb_period_base + int_(hb_period_spread * random_()))
                        if not activity:
                            # Heartbeat gap
                            # Try to send a heartbeat
                            if not listener_req.poll(hb_timeout, POLLOUT):
                                # Must be mute... dead broker?
                                self.logger.warn("Mute req socket: dead broker? bootstrapping")
                                raise BootstrapNow
                            else:
                                listener_req.send(HEARTBEAT_)
                                if not listener_req.poll(hb_timeout):
                                    # Dead again
                                    self.logger.warn("No reply to heartbeat: dead broker? bootstrapping")
                                    raise BootstrapNow
                                else:
                                    # Alive... lets validate the heartbeat pong and move on
                                    if listener_req.recv() != HEARTBEAT_:
                                        # Bad bad bad
                                        self.logger.error("IPSub: bad heartbeat")
                            owner._idle()
                            break
                        tic_count -= 1
                        if tic_count < 0:
                            owner._tic()
                            tic_count = 100
                        for socket, what in activity:
                            if socket is pull:
                                pack = pull_recv_multipart(copy = F)
                                if len_(pack) > 1:
                                    # ^ else Wakeup call, ignore
                                    put_nowait(pack)
                                elif fbuffer_(pack[0]) == buffer_("tic"):
                                    tic_count = 0
                                del pack
                            elif socket is listener_req:
                                if what & POLLOUT:
                                    if not no_updates():
                                        send_update(listener_req)
                                    else:
                                        poller_register(listener_req, POLLIN)
                                if what & POLLIN:
                                    recv_update_reply(listener_req)
                            elif socket is listener_sub and what & POLLIN:
                                recv_update(listener_sub)
                except Queue.Full:
                    self.logger.error("While handling IPSub FSM pipe: Queue full, update lost")
                except BootstrapNow:
                    self.transition(IPSub.FSM.Bootstrap)
                except:
                    self.logger.error("Exception in IPSub listener, re-bootstrapping in a sec", exc_info = True)
                    time.sleep(1)
                    self.transition(IPSub.FSM.Bootstrap)
            
            def leave(self):
                owner = self._owner()
                if owner is not None:
                    owner._notify_all(EVENT_LEAVE_LISTENER, None)
                    owner._disconnect()
        
        class DesignatedBroker(State):
            def enter(self):
                owner = self._owner()
                self.broker_rep = broker_rep = owner._repsocket()
                self.broker_pub = broker_pub = owner._pubsocket()
                self.pull = pull = owner._pullsocket()
                self.poller = poller = zmq.Poller()
                poller.register(broker_pub, zmq.POLLOUT)
                poller.register(broker_rep, zmq.POLLIN)
                poller.register(pull, zmq.POLLIN)
                owner._notify_all(EVENT_ENTER_BROKER, None)

            def stay(self):
                owner = self._owner()

                # Cache globals and attributes, to avoid memory allocations in the hottest loop of all
                broker_rep = self.broker_rep
                broker_pub = self.broker_pub
                pull = self.pull
                poller = self.poller
                POLLOUT = zmq.POLLOUT
                POLLIN = zmq.POLLIN
                HEARTBEAT_ = FRAME_HEARTBEAT
                T = True
                F = False
                poller_poll = poller.poll
                poller_register = poller.register
                poller_unregister = poller.unregister
                pull_recv_multipart = pull.recv_multipart
                no_updates = owner.updates.empty
                put_nowait = owner.updates.put_nowait
                send_update = owner._send_update
                int_ = int
                len_ = len
                random_ = random.random
                tic_count = 100
                buffer_ = buffer
                fbuffer_ = fbuffer
                hb_period_base = owner.heartbeat_avg_period / 2
                hb_period_spread = hb_period_base * 2

                # Poll sockets
                try:
                    while not owner.stop:
                        if not no_updates():
                            poller_register(broker_pub, POLLOUT)
                        activity = poller_poll(hb_period_base + int_(hb_period_spread * random_()))
                        if not activity:
                            broker_pub.send(HEARTBEAT_)
                            owner._idle()
                            break
                        tic_count -= 1
                        if tic_count < 0:
                            owner._tic()
                            tic_count = 100
                        for socket, what in activity:
                            if socket is pull:
                                pack = pull_recv_multipart(copy = F)
                                if len_(pack) > 1:
                                    # ^ else Wakeup call, ignore
                                    put_nowait(pack)
                                elif fbuffer_(pack[0]) == buffer_("tic"):
                                    tic_count = 0
                                del pack
                            elif socket is broker_rep and what & POLLIN:
                                owner._handle_update_request(broker_rep)
                            elif socket is broker_pub and what & POLLOUT:
                                if not no_updates():
                                    send_update(broker_pub, noreply = T)
                                else:
                                    poller_unregister(broker_pub)
                except Queue.Full:
                    self.logger.error("While handling IPSub FSM pipe: Queue full, update lost")
                except BootstrapNow:
                    self.transition(IPSub.FSM.Bootstrap)
                except:
                    self.logger.error("Exception in IPSub broker, re-bootstrapping in a sec", exc_info = True)
                    time.sleep(1)
                    self.transition(IPSub.FSM.Bootstrap)
    
            def leave(self):
                owner = self._owner()
                if owner is not None:
                    owner._notify_all(EVENT_LEAVE_BROKER, None)
                    owner._unbind()

    def __init__(self, broker_addresses, subscriptions = (), ctx=None):
        self.broker_addresses = broker_addresses
        self.updates = Queue.Queue(INPROC_HWM)
        self.current_update = None
        self.listeners = defaultdict(lambda : defaultdict(set))
        
        self.listener_req = self.listener_sub = None
        self.broker_rep = self.broker_pub = None
        self.local = threading.local()
        self._ndebug = None
        self._needs_subscriptions = True

        self.logger = logging.getLogger('chorde.ipsub')
        
        self.subscriptions = set(subscriptions)
        self.subscriptions.add(FRAME_HEARTBEAT)
        self.current_subscriptions = set()
        self.identity = "%x-%x-%s" % (
            os.getpid(),
            id(self),
            os.urandom(8).encode("base64").strip('\t =\n'),
        )

        self.last_idle = time.time()
        self.last_tic = time.time()

        self.heartbeat_avg_period = 500
        self.heartbeat_push_timeout = 4000
        self.fsm_thread_id = None

        self.__context = ctx
        self.__context_lock = threading.Lock()

        self.reset()

    @property
    def context(self):
        if self.__context is None:
            with self.__context_lock:
                if self.__context is None:
                    self.__context = zmq.Context()
        return self.__context

    def reset(self):
        self.fsm = IPSub.FSM.Bootstrap(self)

    def run(self):
        # Must start in bootstrap
        assert not self.is_running

        # Initialize context by touching
        self.context

        self.stop = False
        while not self.stop:
            try:
                self.fsm_thread_id = thread.get_ident()
                self.fsm.enter()
                try:
                    while not self.stop:
                        self.fsm.stay()
                finally:
                    try:
                        self.fsm.leave()
                    except:
                        self.logger.error("Error cleaing up IPSub runner state", exc_info = True)
            except:
                if not self.stop:
                    self.logger.error("Uncaught exception in IPSub runner, resetting and relaunching", exc_info = True)
                self.reset()
            finally:
                self.fsm_thread_id = None

    def terminate(self):
        self.stop = True

    def _bind(self):
        ctx = self.context
        
        pub = ctx.socket(zmq.PUB)
        rep = ctx.socket(zmq.REP)
        set_hwm(pub, BROKER_PUB_HWM)
        set_hwm(rep, BROKER_REP_HWM)
        for addr in self.broker_addresses:
            pub_addr = addr.get('pub')
            rep_addr = addr.get('rep')
            if pub_addr:
                pub.bind(pub_addr)
            if rep_addr:
                rep.bind(rep_addr)

        self.broker_pub = pub
        self.broker_rep = rep

    def _repsocket(self):
        return self.broker_rep

    def _pubsocket(self):
        return self.broker_pub

    def _pullsocket(self):
        local = self.local
        if not hasattr(local, 'pull_socket'):
            ctx = self.context
            pull_socket = ctx.socket(zmq.PULL)
            set_hwm(pull_socket, INPROC_HWM)
            pull_socket.bind("inproc://IPSub%08x_queue" % id(self))
            local.pull_socket = pull_socket
        else:
            pull_socket = local.pull_socket
        return pull_socket

    def _pushsocket(self):
        local = self.local
        if not hasattr(local, 'push_socket'):
            ctx = self.context
            push_socket = ctx.socket(zmq.PUSH)
            set_hwm(push_socket, INPROC_HWM)
            push_socket.connect("inproc://IPSub%08x_queue" % id(self))
            local.push_socket = push_socket
        else:
            push_socket = local.push_socket
        return push_socket

    def _unbind(self):
        try:
            self.broker_pub.close()
        except:
            pass
        try:
            self.broker_rep.close()
        except:
            pass
        self.broker_pub = None
        self.broker_rep = None

    def _connect(self):
        ctx = self.context

        sub = ctx.socket(zmq.SUB)
        req = ctx.socket(zmq.REQ)
        set_hwm(sub, LISTENER_SUB_HWM)
        set_hwm(req, LISTENER_REQ_HWM)
        for addr in self.broker_addresses:
            pub_addr = addr.get('pub')
            rep_addr = addr.get('rep')
            if pub_addr:
                sub.connect(pub_addr)
            if rep_addr:
                req.connect(rep_addr)
            

        self.listener_sub = sub
        self.listener_req = req
        self.current_update = None
        return req

    def _disconnect(self):
        try:
            self.listener_sub.close()
        except:
            pass
        try:
            self.listener_req.close()
        except:
            pass
        self.listener_sub = None
        self.listener_req = None
        self.current_update = None

    def _subscribe(self):
        assert self.listener_sub is not None

        self._needs_subscriptions = False
        sub = self.listener_sub
        for prefix in self.subscriptions - self.current_subscriptions:
            sub.setsockopt(zmq.SUBSCRIBE, prefix)
        for prefix in self.current_subscriptions - self.subscriptions:
            sub.setsockopt(zmq.UNSUBSCRIBE, prefix)

        return sub
    
    def add_subscriptions(self, prefixes):
        self._needs_subscriptions = True
        self.subscriptions.update(prefixes)
        self.wake()
    
    def cancel_subscriptions(self, prefixes):
        self._needs_subscriptions = True
        self.subscriptions -= set(prefixes)
        self.wake()
    
    @property
    def has_updates(self):
        return not self.updates.empty()

    def _send_update(self, socket, noreply = False):
        if self.current_update is not None:
            # Waiting for a reply
            self.logger.error("IPSub FSM error: cannot send update when waiting for a reply, rebootstrapping")
            try:
                self.updates.put_nowait(self.current_update)
            except Queue.Full:
                self.logger.error("While handling IPSub FSM error: Queue full, update lost")
            raise BootstrapNow
        
        try:
            update = self.updates.get_nowait()
        except Queue.Empty:
            # So what...
            return
        
        socket.send_multipart(update, copy = False)

        if not noreply:
            # Remember, we'll wait for a reply
            self.current_update = update
            
        # Notify listeners
        self._notify_all(EVENT_UPDATE_SENT, update)

    def _idle(self):
        if EVENT_IDLE in self.listeners:
            # Rate-limit idle events
            if time.time() >= (self.last_idle + IDLE_PERIOD):
                self._notify_all(EVENT_IDLE, None)
                self.last_idle = time.time()

        # Take the opportunity to check tic timestamp
        self._tic()

    def _tic(self):
        if EVENT_TIC in self.listeners:
            # Rate-limit tic events
            if time.time() >= (self.last_tic + TIC_PERIOD):
                self._notify_all(EVENT_TIC, None)
                self.last_tic = time.time()

    def request_tic(self):
        try:
            self._pushsocket().send("tic")
        except zmq.ZMQError:
            # Shit happens, probably not connected
            pass
    
    def _recv_update(self, socket):
        update = socket.recv_multipart(copy = False)

        if not self._check_heartbeat(update):
            # Notify listeners
            self._notify_all(EVENT_INCOMING_UPDATE, update)
        else:
            self._idle()

    def _recv_update_reply(self, socket):
        # Check format without copying, assertion failures result in re-bootstrapping
        reply = socket.recv_multipart(copy = False)

        if ( MIN_UPDATE_REPLY_FRAMES <= len(reply) <= MAX_UPDATE_REPLY_FRAMES
                 and len(reply[0]) <= MAX_UPDATE_REPLY_FIRSTFRAME ):
             reply_code = reply[0].bytes
             if reply_code not in FRAME_VALID_UPDATE_REPLIES:
                 # Must be a reply payload, which implicitly means OK
                 reply_code = FRAME_UPDATE_OK
        else:
             reply_code = FRAME_UPDATE_OK
        
        # Notify listeners
        self._notify_all(EVENT_FOR_REPLY[reply_code], (self.current_update, reply))
        self.current_update = None

    def _check_heartbeat(self, update):
        HEARTBEAT_ = FRAME_HEARTBEAT
        
        # Fast, non-copying check for a heartbeat frame
        return (
            len(update) == 1 
            and (len(update[0])) == len(HEARTBEAT_) 
            and update[0].bytes == HEARTBEAT_
        )

    def _handle_update_request(self, socket, 
            isinstance=isinstance, BrokerReply=BrokerReply):
        update = socket.recv_multipart(copy = False)
        if self._check_heartbeat(update):
            # Got a heartbeat, reply in kind
            socket.send(FRAME_HEARTBEAT)
            self._idle()
        else:
            # Real update, handle it
            try:
                self.updates.put_nowait(update)
                dropped = False
            except Queue.Full:
                dropped = True
            
            # Notify listeners
            rv = self._notify_all(EVENT_INCOMING_UPDATE, update)
            if isinstance(rv, BrokerReply):
                socket.send_multipart(rv.reply, copy = False)
            elif dropped:
                socket.send(FRAME_UPDATE_DROPPED)
            else:
                socket.send(FRAME_UPDATE_OK)


    def publish_json(self, prefix, payload, copy = False):
        self.publish(prefix, ['json',ENCODINGS['json'](payload)], copy)

    def publish_pyobj(self, prefix, payload, copy = False):
        self.publish(prefix, ['pyobj',ENCODINGS['pyobj'](payload)], copy)

    def publish_bytes(self, prefix, payload, copy = False):
        self.publish(prefix, ['bytes',ENCODINGS['bytes'](payload)], copy)

    def publish_unicode(self, prefix, payload, copy = False):
        self.publish(prefix, ['utf8',ENCODINGS['utf8'](payload)], copy)

    def publish_encode(self, prefix, encoding, payload, copy = False):
        self.publish(prefix, self.encode_payload(encoding, payload), copy)

    @staticmethod
    def register_encoding(name, encoder, decoder, stream_decoder):
        """
        Register an encoding with the specified name.
    
        Params:
            encoder: a callable that takes the object to be dumped,
                and returns a string or buffer object.
            decoder: a callable that takes a string resulting of encoder,
                and returns in the represented object
            stream_decoder: like decoder, but instead will take a
                file-like object.
        """
        ENCODINGS[name] = encoder
        DECODINGS[name] = decoder
        STREAMDECODINGS[name] = stream_decoder

    @staticmethod
    def register_pyobj(pickler, unpickler):
        """
        Registers a pickling encoding.

        Params:
            pickler, unpickler: pickler/unpickler factory callables
                that take a file-like object to dump into. Can be
                stdlib's Pickle/Unpickle classes, or cPickle's, or
                sPickles, they both work out-of-the-box.
        """
        def dumps(x):
            io = cStringIO.StringIO()
            p = pickler(io,2)
            p.dump(x)
            return io.getvalue()
        def loads(x):
            io = cStringIO.StringIO(x)
            p = unpickler(io)
            return p.load()
        def load(x):
            return unpickler(x).load()
        IPSub.register_encoding('pyobj', dumps, loads, load)
    
    @staticmethod
    def register_default_pyobj():
        IPSub.register_pyobj(cPickle.Pickler, cPickle.Unpickler)
    
    @staticmethod
    def encode_payload(encoding, payload):
        return [encoding, ENCODINGS[encoding](payload)]

    @staticmethod
    def decode_payload(payload):
        encoding = payload[-2].bytes
        payload = payload[-1]
        payload = cStringIO.StringIO(fbuffer(payload))
        return STREAMDECODINGS[encoding](payload)

    def publish(self, prefix, payload, copy = False, _ident = thread.get_ident):
        parts = [ prefix, self.identity ] + payload
        if _ident() == self.fsm_thread_id:
            try:
                self.updates.put_nowait(parts)
            except Queue.Full:
                self.logger.error("While handling re-entrant IPSub publication: Queue full, update lost")
        else:
            push = self._pushsocket()
            if push.poll(self.heartbeat_push_timeout, zmq.POLLOUT):
                push.send_multipart(parts, copy = copy)
            else:
                self.logger.error("While handling IPSub publication: Push socket timeout, update lost")

    def wake(self):
        if self.__context is not None:
            try:
                # Don't block, mute means awaken and not catching up anyway
                push = self._pushsocket()
                if push.poll(1, zmq.POLLOUT):
                    push.send("")
            except zmq.ZMQError:
                # Shit happens, probably not connected
                pass

    def listen(self, prefix, event, callback):
        """
        Registers a listener for all events whose prefix starts
        with the given prefix.

        The callback will be invoked with the whole prefix as
        first argument, or None if the event doesn't have one,
        the event id as second argument, and the whole message,
        including prefix and payload, as third argument. 
        Use decode_payload to decode, if needed.

        It should return True, if it is to be called again, or
        False if the listener is to be removed. Designated
        brokers can also return a BrokerReply wrapper, in which case 
        the reply's payload will be returned to the listener where 
        the update originated, providing a way to piggy-back the 
        req-response connection among them. These are considered
        as True, so they will not be automatically removed.

        Listeners are not guaranteed to be called in any specific
        or stable order, but they are guaranteed to be called just
        once (per instance, not function name). They should return fast, 
        or the I/O thread may stall.
        """
        self.listeners[event][prefix].add(callback)
        if event in IDENTITY_EVENTS:
            # Those are external, so we must subscribe
            self.add_subscriptions((prefix,))
    
    def listen_decode(self, prefix, event, callback):
        """
        See listen. The difference is that in this case, the payload
        in decoded form will be given to the callback, rather than
        the entire message.

        Returns the actual callback to be used for unlistening.
        """
        def decoding_callback(prefix, event, message):
            return callback(prefix, event, IPSub.decode_payload(message))
        self.listen(prefix, event, decoding_callback)
        return decoding_callback

    def unlisten(self, prefix, event, callback):
        if prefix in self.listeners[event]:
            try:
                self.listeners[event][prefix].remove(callback)
            except KeyError:
                pass
        if event in IDENTITY_EVENTS and not self.listeners[event].get(prefix):
            # Not interesting anymore... unsubscribe
            self.cancel_subscriptions((prefix,))

    @property
    def is_broker(self):
        return self.fsm.__class__ is IPSub.FSM.DesignatedBroker

    @property
    def is_running(self):
        return self.fsm.__class__ is not IPSub.FSM.Bootstrap

    def _notify_all(self, event, update):
        listeners = self.listeners.get(event)
        if not listeners and self._ndebug:
            return
        
        if event in IDENTITY_EVENTS and len(update) > 1:
            identity = update[1].bytes
        else:
            identity = None

        if self._ndebug is None:
            self._ndebug = not self.logger.isEnabledFor(logging.DEBUG)

        if not self._ndebug:
            if identity is None and event != EVENT_UPDATE_SENT:
                prefix = None
            elif len(update[0]) < MAX_PREFIX:
                prefix = getattr(update[0], 'bytes', update[0])
            else:
                prefix = bbytes(fbuffer(update[0], 0, MAX_PREFIX))
            if identity is None or identity == self.identity:
                self.logger.debug("IPSub: (from %r) %s (prefix %r)", self.identity, EVENT_NAMES[event], prefix)
            else:
                self.logger.debug("IPSub: (from %r) %s (prefix %r)", identity, EVENT_NAMES[event], prefix)

        if identity is not None and identity == self.identity:
            # Ehm... identified roundtrip -> ignore
            return
        
        if listeners:
            if identity is None:
                prefix = None
            elif len(update[0]) < MAX_PREFIX:
                prefix = update[0].bytes
            else:
                prefix = bbytes(fbuffer(update[0], 0, MAX_PREFIX))
            called = set()
            rv = None
            for cb_prefix, callbacks in listeners.items():
                if prefix is None or prefix.startswith(cb_prefix):
                    byebye = set()
                    for callback in set(callbacks):
                        if callback in called:
                            continue
                        try:
                            rv = callback(prefix, event, update)
                            if not rv:
                                byebye.add(callback)
                            else:
                                called.add(callback)
                        except:
                            self.logger.error("Exception in handler", exc_info = True)
                            byebye.add(callback)
                    for callback in byebye:
                        self.unlisten(cb_prefix, event, callback)
            return rv


