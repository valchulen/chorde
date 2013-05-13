# -*- coding: utf-8 -*-
import threading
import time
import unittest
import logging

import zmq

import chorde.mq.ipsub as ipsub
import chorde.mq.coherence as coherence
import chorde.clients.inproc as inproc

class CoherenceProtocolTest(unittest.TestCase):
    def setUp(self):
        ipsub.IPSub.register_default_pyobj()
        
        ctx = zmq.Context.instance()
        s1 = ctx.socket(zmq.REQ)
        s2 = ctx.socket(zmq.REQ)
        port1 = s1.bind_to_random_port("tcp://127.0.0.1")
        port2 = s2.bind_to_random_port("tcp://127.0.0.1")
        s1.close()
        s2.close()
        del s1,s2
        logging.debug("ipsub1 ports: %d, %d", port1, port2)
        
        self.ipsub = ipsub.IPSub([dict(rep="tcp://127.0.0.1:%d" % port1, pub="tcp://127.0.0.1:%d" % port2)])
        self.ipsub_thread = threading.Thread(target=self.ipsub.run)
        self.ipsub_thread.daemon = True
        
        self.ipsub2 = ipsub.IPSub([dict(rep="tcp://127.0.0.1:%d" % port1, pub="tcp://127.0.0.1:%d" % port2)])
        self.ipsub2_thread = threading.Thread(target=self.ipsub2.run)
        self.ipsub2_thread.daemon = True

        self.private = inproc.InprocCacheClient(100)
        self.private2 = inproc.InprocCacheClient(100)
        self.shared = inproc.InprocCacheClient(100)

        self.coherence = coherence.CoherenceManager('ns', self.private, self.shared, self.ipsub)
        self.coherence2 = coherence.CoherenceManager('ns', self.private2, self.shared, self.ipsub2)

        time.sleep(0.1)
        
        self.ipsub_thread.start()
        self.ipsub2_thread.start()
        
        time.sleep(0.1)

    def tearDown(self):
        self.ipsub.terminate()
        self.ipsub2.terminate()
        self.ipsub.wake()
        self.ipsub2.wake()
        del self.coherence, self.coherence2
        del self.private, self.private2, self.shared
        self.ipsub_thread.join(5000)
        self.ipsub2_thread.join(5000)
        del self.ipsub, self.ipsub_thread, self.ipsub2, self.ipsub2_thread

    def test_deletes(self):
        self.private.put(1, 1, 600)
        self.private.put(2, 2, 600)
        self.private2.put(1, 1, 600)
        self.private2.put(2, 2, 600)
        self.private.delete(1)
        self.coherence.fire_deletion(1)
        time.sleep(0.1)
        self.assertTrue(not self.private2.contains(1))
