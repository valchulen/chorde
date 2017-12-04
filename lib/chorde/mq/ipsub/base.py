# -*- coding: utf-8 -*-
import time
import zmq
import logging
import json
import os
from collections import defaultdict

try:
    import cPickle
except ImportError:
    import pickle as cPickle  # lint:ok

try:
    import cStringIO
except ImportError:
    import StringIO as cStringIO  # lint:ok

__ALL__ = (
    'EVENT_INCOMING_UPDATE',
    'EVENT_UPDATE_ACKNOWLEDGED',
    'EVENT_UPDATE_IGNORED',
    'EVENT_UPDATE_SENT',
    'EVENT_ENTER_BROKER',
    'EVENT_LEAVE_BROKER',
    'EVENT_ENTER_LISTENER',
    'EVENT_LEAVE_LISTENER',
    'EVENT_IDLE',
    'EVENT_TIC',
    'EVENT_NAMES',

    'FRAME_UPDATE_OK',
    'FRAME_UPDATE_DROPPED',
    'FRAME_VALID_UPDATE_REPLIES',
    'EVENT_FOR_REPLY',

    'BrokerReply',
    'BaseIPSub',
)

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

FRAME_UPDATE_OK = "OK"
FRAME_UPDATE_DROPPED = "DROP"
FRAME_VALID_UPDATE_REPLIES = (FRAME_UPDATE_OK, FRAME_UPDATE_DROPPED)

EVENT_FOR_REPLY = {
    FRAME_UPDATE_OK : EVENT_UPDATE_ACKNOWLEDGED,
    FRAME_UPDATE_DROPPED : EVENT_UPDATE_IGNORED,
}

MAX_PREFIX = 256

IDLE_PERIOD = 0.5
TIC_PERIOD = 1.0

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

class BrokerReply(object):
    __slots__ = ('reply',)
    def __init__(self, *reply_parts):
        self.reply = reply_parts

class BaseIPSub(object):
    # Implementations are free to provide alternative versions as static/classmethods
    def __init__(self):
        self.logger = logging.getLogger('chorde.ipsub')

        self.listeners = defaultdict(lambda : defaultdict(set))

        self.last_idle = time.time()
        self.last_tic = time.time()

        self.identity = "%x-%x-%s" % (
            os.getpid(),
            id(self),
            os.urandom(8).encode("base64").strip('\t =\n'),
        )

    def _idle(self):
        """
        Implementations SHOULD call this method when they've had no
        messages to process for a while heartbeat period (approximately).
        """
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
        """
        Request that an EVENT_TIC be issued at the next oportunity
        """
        try:
            self.last_tic = time.time() - TIC_PERIOD
            self._pushsocket().send("tic")
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
            return callback(prefix, event, self.decode_payload(message))
        self.listen(prefix, event, decoding_callback)
        return decoding_callback

    def unlisten(self, prefix, event, callback):
        """
        Removes the specified listener. See listen.
        """
        if prefix in self.listeners[event]:
            try:
                self.listeners[event][prefix].remove(callback)
            except KeyError:
                pass
        if event in IDENTITY_EVENTS and not self.listeners[event].get(prefix):
            # Not interesting anymore... unsubscribe
            self.cancel_subscriptions((prefix,))

    def _notify_all(self, event, update):
        """
        Notifies all listeners of the given event, with the payload being "update".
        See publish on the format of the payload.
        """
        listeners = self.listeners.get(event)
        if not listeners and self._ndebug:
            return

        if event in IDENTITY_EVENTS and len(update) > 1:
            identity = update[1]
        else:
            identity = None

        if self._ndebug is None:
            self._ndebug = not self.logger.isEnabledFor(logging.DEBUG)

        if not self._ndebug:
            if identity is None and event != EVENT_UPDATE_SENT:
                prefix = None
            elif len(update[0]) < MAX_PREFIX:
                prefix = update[0]
            else:
                prefix = update[0][:MAX_PREFIX]
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
                prefix = update[0]
            else:
                prefix = update[0][:MAX_PREFIX]
            called = set()
            rrv = rv = None
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
                                if isinstance(rv, BrokerReply):
                                    rrv = rv
                        except:
                            self.logger.error("Exception in handler", exc_info = True)
                            byebye.add(callback)
                    for callback in byebye:
                        self.unlisten(cb_prefix, event, callback)
            return rrv or rv

    def add_subscriptions(self, prefixes):
        """
        Mark the given set of prefixes as "interesting".
        If the underlying channel support filtering by message prefix, these
        prefixes that haven been added are the ones that must be captured.

        Implementations must provide this method.
        """
        raise NotImplementedError

    def cancel_subscriptions(self, prefixes):
        """
        Mark the given set of prefixes as "uninteresting".
        If the underlying channel support filtering by message prefix, these
        prefixes should not be captured any more.

        Implementations must provide this method.
        """
        raise NotImplementedError

    def publish_json(self, prefix, payload, timeout = None):
        """
        Publish the message using the 'json' encoding. See publish.
        """
        self.publish(prefix, ['json',ENCODINGS['json'](payload)], timeout)

    def publish_pyobj(self, prefix, payload, timeout = None):
        """
        Publish the message using the 'pyobj' encoding. See publish.
        """
        self.publish(prefix, ['pyobj',ENCODINGS['pyobj'](payload)], timeout)

    def publish_bytes(self, prefix, payload, timeout = None):
        """
        Publish the message using the 'bytes' encoding. See publish.
        """
        self.publish(prefix, ['bytes',ENCODINGS['bytes'](payload)], timeout)

    def publish_unicode(self, prefix, payload, timeout = None):
        """
        Publish the message using the 'utf8' encoding. See publish.
        """
        self.publish(prefix, ['utf8',ENCODINGS['utf8'](payload)], timeout)

    def publish_encode(self, prefix, encoding, payload, timeout = None):
        """
        Publish the message using a custom encoding (must be registered with register_encoding).
        See publish, register_encoding.
        """
        self.publish(prefix, self.encode_payload(encoding, payload), timeout)

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
        BaseIPSub.register_encoding('pyobj', dumps, loads, load)

    @staticmethod
    def register_default_pyobj():
        BaseIPSub.register_pyobj(cPickle.Pickler, cPickle.Unpickler)

    @staticmethod
    def encode_payload(encoding, payload):
        return [encoding, ENCODINGS[encoding](payload)]

    @classmethod
    def decode_payload(cls, payload):
        encoding = payload[-2]
        payload = payload[-1]
        return DECODINGS[encoding](payload)

    def publish(self, prefix, payload, copy = False, timeout = None):
        """
        Publish a message, with the given prefix and payload.

        If copy is True, a copy of the payload is made (especially useful if the payload comes
        from another message and contains reusable buffers instead of simple strings, for which
        copy=True would be a no-op).

        If timeout is given and not None, it specifies a maximum time to block in milliseconds
        while attempting to push the message. If the timeout expires an exception will be thrown.

        Implementations must provide this method.
        """
        raise NotImplementedError

