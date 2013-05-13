# -*- coding: utf-8 -*-
import threading
import time
import unittest
import logging

import zmq

import chorde.mq.ipsub as ipsub

class IPSubTest(unittest.TestCase):
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

        time.sleep(0.1)
        
        self.ipsub_thread.start()
        self.ipsub2_thread.start()
        
        time.sleep(0.1)

    def tearDown(self):
        self.ipsub.terminate()
        self.ipsub2.terminate()
        self.ipsub_thread.join(5000)
        self.ipsub2_thread.join(5000)
        del self.ipsub, self.ipsub_thread, self.ipsub2, self.ipsub2_thread

    def test_simple_pub_no_sub(self):
        self.ipsub.publish_pyobj('msg',None)
        self.ipsub2.publish_pyobj('msg',None)

    def test_simple_pub_with_sub(self):
        updates = []
        def listener(prefix, identity, message):
            updates.append((prefix, identity, message))
            return True
        self.ipsub2.listen('', ipsub.EVENT_INCOMING_UPDATE, listener)
        time.sleep(0.1)
        self.ipsub.publish_pyobj('msg',None)
        self.ipsub.publish_pyobj('msg',None)
        time.sleep(0.1)
        self.assertEqual(len(updates), 2)

    def test_simple_pub_with_sub_r(self):
        updates = []
        def listener(prefix, identity, message):
            updates.append((prefix, identity, message))
            return True
        self.ipsub.listen('', ipsub.EVENT_INCOMING_UPDATE, listener)
        time.sleep(0.1)
        self.ipsub2.publish_pyobj('msg',None)
        self.ipsub2.publish_pyobj('msg',None)
        time.sleep(0.1)
        self.assertEqual(len(updates), 2)

    def test_simple_pub_with_sub_autoremove(self):
        updates = []
        def listener(prefix, identity, message):
            updates.append((prefix, identity, message))
            return False
        self.ipsub2.listen('', ipsub.EVENT_INCOMING_UPDATE, listener)
        time.sleep(0.1)
        self.ipsub.publish_pyobj('msg',None)
        self.ipsub.publish_pyobj('msg',None)
        time.sleep(0.1)
        self.assertEqual(len(updates), 1)

    def test_simple_pub_with_sub_prefix(self):
        updates = []
        def listener(prefix, identity, message):
            updates.append((prefix, identity, message))
            return True
        self.ipsub2.listen('msg', ipsub.EVENT_INCOMING_UPDATE, listener)
        time.sleep(0.1)
        self.ipsub.publish_pyobj('msg',None)
        self.ipsub.publish_pyobj('mog',None)
        time.sleep(0.1)
        self.assertEqual(len(updates), 1)

    def test_simple_pub_with_sub_prefix_r(self):
        updates = []
        def listener(prefix, identity, message):
            updates.append((prefix, identity, message))
            return True
        self.ipsub.listen('msg', ipsub.EVENT_INCOMING_UPDATE, listener)
        time.sleep(0.1)
        self.ipsub2.publish_pyobj('msg',None)
        self.ipsub2.publish_pyobj('mog',None)
        time.sleep(0.1)
        self.assertEqual(len(updates), 1)

