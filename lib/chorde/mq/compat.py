# -*- coding: utf-8 -*-
import zmq

# Test zmq Frame's buffer protocol
try:
    buffer(zmq.Message(''))
    fbuffer = buffer
    bbytes = bytes
except TypeError:
    def fbuffer(frame):
        return frame.buffer if hasattr(frame, 'buffer') else buffer(frame)
    def bbytes(buf):
        return buf.tobytes() if hasattr(buf, 'tobytes') else bytes(buf)
