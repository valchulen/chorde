# -*- coding: utf-8 -*-
import weakref
import time
import zmq
import logging
import Queue
import threading
import json
import os
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

FRAME_HEARTBEAT = "__HeyDude__"
FRAME_UPDATE_OK = "OK"
FRAME_UPDATE_DROPPED = "DROP"
FRAME_VALID_UPDATE_REPLIES = (FRAME_UPDATE_OK, FRAME_UPDATE_DROPPED)

EVENT_INCOMING_UPDATE = 1
EVENT_UPDATE_ACKNOWLEDGED = 2
EVENT_UPDATE_IGNORED = 3
EVENT_UPDATE_SENT = 4

EVENT_NAMES = {
    EVENT_INCOMING_UPDATE : 'INCOMING_UPDATE',
    EVENT_UPDATE_ACKNOWLEDGED : 'UPDATE_ACKNOWLEDGED',
    EVENT_UPDATE_IGNORED: 'UPDATE_DROPPED',
    EVENT_UPDATE_SENT : 'UPDATE_SENT',
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

EVENT_FOR_REPLY = {
    FRAME_UPDATE_OK : EVENT_UPDATE_ACKNOWLEDGED,
    FRAME_UPDATE_DROPPED : EVENT_UPDATE_IGNORED,
}

ENCODINGS = {
    'json' : lambda x : json.dumps(x, separators=(',',':')),
    'pyobj' : lambda x : cPickle.dumps(x, 2),
    'bytes' : lambda x : x,
    'utf8' : lambda x : x.encode('utf8') if isinstance(x, unicode) else x,
}

DECODINGS = {
    'json' : json.loads,
    'pyobj' : cPickle.loads,
    'bytes' : lambda x : x,
    'utf8' : lambda x : x.decode('utf8'),
}

STREAMDECODINGS = {
    'json' : json.load,
    'pyobj' : cPickle.load,
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

class IPSub(object):
    class FSM(object):
        class State(object):
            __metaclass__ = ABCMeta

            def __init__(self, owner):
                self._owner = weakref.ref(owner)

            def transition(self, newstate, data = None):
                logging.debug("IPSub.FSM: LEAVE %s", self.__class__.__name__)
                self.leave(data)
                self.__class__ = newstate
                logging.debug("IPSub.FSM: ENTER %s", self.__class__.__name__)
                self.enter(data)

            @abstractmethod
            def enter(self, data = None):
                pass

            @abstractmethod
            def leave(self, data = None):
                pass

            @abstractmethod
            def stay(self):
                pass
        
        class Bootstrap(State):
            def enter(self, data = None):
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
                        logging.info("Got %s connecting", e)
                        logging.debug("Got %s connecting", e, exc_info = True)
                        time.sleep(0.2)
                else:
                    try:
                        owner._bind()
                    except:
                        return self.transition(IPSub.FSM.Listener)
                self.transition(IPSub.FSM.DesignatedBroker)

            def stay(self):
                pass

            def leave(self, data = None):
                pass
        
        class Listener(State):
            def enter(self, data = None):
                owner = self._owner()
                self.listener_req = listener_req = owner._connect()
                self.listener_sub = listener_sub = owner._subscribe()
                self.pull = pull = owner._pullsocket()
                self.poller = poller = zmq.Poller()
                poller.register(listener_sub, zmq.POLLIN)
                poller.register(pull, zmq.POLLIN)
                poller.register(listener_req, zmq.POLLIN | zmq.POLLOUT)

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

                # Poll sockets
                try:
                    while not owner.stop:
                        if not no_updates():
                            poller_register(listener_req, POLLIN|POLLOUT)
                        activity = poller_poll(1000)
                        if not activity:
                            # Heartbeat gap
                            # Try to send a heartbeat
                            if not listener_req.poll(1000, POLLOUT):
                                # Must be mute... dead broker?
                                raise BootstrapNow
                            else:
                                listener_req.send(HEARTBEAT_)
                                if not listener_req.poll(2000):
                                    # Dead again
                                    raise BootstrapNow
                                else:
                                    # Alive... lets validate the heartbeat pong and move on
                                    if listener_req.recv() != HEARTBEAT_:
                                        # Bad bad bad
                                        logging.error("IPSub: bad heartbeat")
                            break
                        for socket, what in activity:
                            if socket is pull:
                                put_nowait(pull_recv_multipart(copy = F))
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
                except BootstrapNow:
                    self.transition(IPSub.FSM.Bootstrap)
                except:
                    logging.error("Exception in IPSub listener, re-bootstrapping in a sec", exc_info = True)
                    time.sleep(1)
                    self.transition(IPSub.FSM.Bootstrap)
            
            def leave(self, data = None):
                self._owner()._disconnect()
        
        class DesignatedBroker(State):
            def enter(self, data = None):
                owner = self._owner()
                self.broker_rep = broker_rep = owner._repsocket()
                self.broker_pub = broker_pub = owner._pubsocket()
                self.pull = pull = owner._pullsocket()
                self.poller = poller = zmq.Poller()
                poller.register(broker_pub, zmq.POLLOUT)
                poller.register(broker_rep, zmq.POLLIN)
                poller.register(pull, zmq.POLLIN)

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

                # Poll sockets
                try:
                    while not owner.stop:
                        if not no_updates():
                            poller_register(broker_pub, POLLOUT)
                        activity = poller_poll(500)
                        if not activity:
                            broker_pub.send(HEARTBEAT_)
                            break
                        for socket, what in activity:
                            if socket is pull:
                                put_nowait(pull_recv_multipart(copy = F))
                            elif socket is broker_rep and what & POLLIN:
                                owner._handle_update_request(broker_rep)
                            elif socket is broker_pub and what & POLLOUT:
                                if not no_updates():
                                    send_update(broker_pub, noreply = T)
                                else:
                                    poller_unregister(broker_pub)
                except BootstrapNow:
                    self.transition(IPSub.FSM.Bootstrap)
                except:
                    logging.error("Exception in IPSub broker, re-bootstrapping in a sec", exc_info = True)
                    time.sleep(1)
                    self.transition(IPSub.FSM.Bootstrap)
    
            def leave(self, data = None):
                self._owner()._unbind()

    def __init__(self, broker_addresses, subscriptions = ()):
        self.broker_addresses = broker_addresses
        self.updates = Queue.Queue(INPROC_HWM)
        self.current_update = None
        self.listeners = defaultdict(lambda : defaultdict(set))
        
        self.listener_req = self.listener_sub = None
        self.broker_rep = self.broker_pub = None
        self.local = threading.local()
        self._ndebug = None
        
        self.subscriptions = set(subscriptions)
        self.subscriptions.add(FRAME_HEARTBEAT)
        self.identity = "%x-%x-%s" % (
            os.getpid(),
            id(self),
            os.urandom(8).encode("base64").strip('\t =\n'),
        )
        
        self.fsm = IPSub.FSM.Bootstrap(self)

    def run(self):
        # Must start in bootstrap
        assert self.fsm.__class__ is IPSub.FSM.Bootstrap
        
        self.stop = False
        self.fsm.enter()
        while not self.stop:
            self.fsm.stay()
        self.fsm.leave()

    def terminate(self):
        self.stop = True

    def _bind(self):
        ctx = zmq.Context.instance()

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
            ctx = zmq.Context.instance()
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
            ctx = zmq.Context.instance()
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
        ctx = zmq.Context.instance()

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

    def _subscribe(self):
        assert self.listener_sub is not None

        sub = self.listener_sub
        for prefix in self.subscriptions:
            sub.setsockopt(zmq.SUBSCRIBE, prefix)

        return sub
    
    def add_subscriptions(self, prefixes):
        if self.listener_sub is None:
            # Ignore
            return

        subscriptions = self.subscriptions
        sub = self.listener_sub
        if sub:
            for prefix in set(prefixes) - set(subscriptions):
                sub.setsockopt(zmq.SUBSCRIBE, prefix)
                subscriptions.add(prefix)
    
    def cancel_subscriptions(self, prefixes):
        if self.listener_sub is None:
            # Ignore
            return

        subscriptions = self.subscriptions
        sub = self.listener_sub
        for prefix in set(prefixes) & set(subscriptions):
            sub.setsockopt(zmq.UNSUBSCRIBE, prefix)
            subscriptions.remove(prefix)
    
    @property
    def has_updates(self):
        return not self.updates.empty()

    def _send_update(self, socket, noreply = False):
        if self.current_update is not None:
            # Waiting for a reply
            logging.error("IPSub FSM error: cannot send update when waiting for a reply, rebootstrapping")
            try:
                self.updates.put_nowait(self.current_update)
            except Queue.Full:
                logging.error("While handling IPSub FSM error: Queue full, update lost")
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

    def _recv_update(self, socket):
        update = socket.recv_multipart(copy = False)

        if not self._check_heartbeat(update):
            # Notify listeners
            self._notify_all(EVENT_INCOMING_UPDATE, update)

    def _recv_update_reply(self, socket):
        # Check format without copying, assertion failures result in re-bootstrapping
        reply = socket.recv_multipart(copy = False)
        assert MIN_UPDATE_REPLY_FRAMES <= len(reply) <= MAX_UPDATE_REPLY_FRAMES
        assert len(reply[0]) <= MAX_UPDATE_REPLY_FIRSTFRAME

        reply_code = reply[0].bytes
        if reply_code not in FRAME_VALID_UPDATE_REPLIES:
            # Ignore
            return
        
        # Notify listeners
        self._notify_all(EVENT_FOR_REPLY[reply_code], self.current_update)
        self.current_update = None

    def _check_heartbeat(self, update):
        HEARTBEAT_ = FRAME_HEARTBEAT
        
        # Fast, non-copying check for a heartbeat frame
        return (
            len(update) == 1 
            and (len(update[0])) == len(HEARTBEAT_) 
            and update[0].bytes == HEARTBEAT_
        )

    def _handle_update_request(self, socket):
        update = socket.recv_multipart(copy = False)
        if self._check_heartbeat(update):
            # Got a heartbeat, reply in kind
            socket.send(FRAME_HEARTBEAT)
        else:
            # Real update, handle it
            try:
                self.updates.put_nowait(update)
                socket.send(FRAME_UPDATE_OK)
            except Queue.Full:
                socket.send(FRAME_UPDATE_DROPPED)
            
            # Notify listeners
            self._notify_all(EVENT_INCOMING_UPDATE, update)

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
    def encode_payload(encoding, payload):
        return [encoding, ENCODINGS[encoding](payload)]

    @staticmethod
    def decode_payload(payload):
        encoding = payload[-2].bytes
        payload = payload[-1]
        payload = cStringIO.StringIO(buffer(payload))
        return STREAMDECODINGS[encoding](payload)

    def publish(self, prefix, payload, copy = False):
        self._pushsocket().send_multipart(
            [ prefix, self.identity ] + payload,
            copy = copy )

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
        False if the listener is to be removed.

        Listeners are not guaranteed to be called in any specific
        or stable order, and should return fast, or the I/O thread
        may stall.
        """
        self.listeners[event][prefix].add(callback)
        if event in IDENTITY_EVENTS:
            # Those are external, so we must subscribe
            self.add_subscriptions((prefix,))
    
    def listen_decode(self, prefix, event, callback):
        """
        See listen. The difference is that in this case, the payload
        in decoded form will be given to the callback, rather than
        the entire message
        """
        def decoding_callback(prefix, event, message):
            return callback(prefix, event, IPSub.decode_payload(message))
        self.listen(prefix, event, decoding_callback)

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
    def isbroker(self):
        return self.fsm.__class__ is IPSub.FSM.DesignatedBroker

    def _notify_all(self, event, update):
        listeners = self.listeners.get(event)
        if not listeners and self._ndebug:
            return
        
        if event in IDENTITY_EVENTS and len(update) > 1:
            identity = update[1].bytes
        else:
            identity = None

        if self._ndebug is None:
            self._ndebug = not logging.getLogger().isEnabledFor(logging.DEBUG)

        if not self._ndebug:
            if identity is None or identity == self.identity:
                logging.debug("IPSub: (from myself) %s", EVENT_NAMES[event])
            else:
                logging.debug("IPSub: (from %r) %s", identity, EVENT_NAMES[event])

        if identity is not None and identity == self.identity:
            # Ehm... identified roundtrip -> ignore
            return
        
        if listeners:
            if identity is None:
                prefix = None
            elif len(update[0]) < MAX_PREFIX:
                prefix = update[0].bytes
            else:
                prefix = bytes(buffer(update[0], 0, MAX_PREFIX))
            for cb_prefix, callbacks in listeners.items():
                if prefix is None or prefix.startswith(cb_prefix):
                    byebye = set()
                    for callback in callbacks:
                        try:
                            if not callback(prefix, event, update):
                                byebye.add(callback)
                        except:
                            logging.error("Exception in handler", exc_info = True)
                            byebye.add(callback)
                    for callback in byebye:
                        self.unlisten(cb_prefix, event, callback)


