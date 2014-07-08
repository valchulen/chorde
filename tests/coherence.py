# -*- coding: utf-8 -*-
import threading
import time
import unittest
import logging

skipIfUnsupported = lambda c : c
zmq = None
ipsub = None
coherence = None
inproc = None

try:
    import zmq
    try:
        import chorde.mq.ipsub as ipsub
        import chorde.mq.coherence as coherence
        import chorde.clients.inproc as inproc
    except ImportError:
        skipIfUnsupported = unittest.skip("No messaging support built in")
except ImportError:
    skipIfUnsupported = unittest.skip("No ZMQ available")

@skipIfUnsupported
class CoherenceProtocolTest(unittest.TestCase):
    coherence_kwargs = {}
    
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
        
        self.ipsub = ipsub.IPSub([dict(rep="tcp://127.0.0.1:%d" % port1, pub="tcp://127.0.0.1:%d" % port2)], ctx=ctx)
        self.ipsub_thread = threading.Thread(target=self.ipsub.run)
        self.ipsub_thread.daemon = True
        
        self.ipsub2 = ipsub.IPSub([dict(rep="tcp://127.0.0.1:%d" % port1, pub="tcp://127.0.0.1:%d" % port2)], ctx=ctx)
        self.ipsub2_thread = threading.Thread(target=self.ipsub2.run)
        self.ipsub2_thread.daemon = True
        
        self.ipsub3 = ipsub.IPSub([dict(rep="tcp://127.0.0.1:%d" % port1, pub="tcp://127.0.0.1:%d" % port2)], ctx=ctx)

        self.private = inproc.InprocCacheClient(100)
        self.private2 = inproc.InprocCacheClient(100)
        self.shared = inproc.InprocCacheClient(100)

        self.coherence = coherence.CoherenceManager('ns', self.private, self.shared, self.ipsub, **self.coherence_kwargs)
        self.coherence2 = coherence.CoherenceManager('ns', self.private2, self.shared, self.ipsub2, **self.coherence_kwargs)
        self.coherence3 = coherence.CoherenceManager('ns', self.private2, self.shared, self.ipsub3, **self.coherence_kwargs)

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

    def test_pendq(self):
        t1 = time.time()
        pending = self.coherence.query_pending(1, lambda:1)
        t2 = time.time()
        
        self.assertEqual(pending, None) # No pending
        self.assertLess(t2-t1, 1.0) # No timeout
        
        t1 = time.time()
        pending = self.coherence2.query_pending(1, lambda:1)
        t2 = time.time()
        
        self.assertEqual(pending, None) # No pending
        self.assertLess(t2-t1, 1.0) # No timeout

    def test_pendq_lock(self):
        t1 = time.time()
        pending = self.coherence2.query_pending(1, lambda:1, optimistic_lock = True)
        t2 = time.time()
        
        self.assertEqual(pending, None) # No pending
        self.assertLess(t2-t1, 1.0) # No timeout
        self.assertTrue(1 in self.coherence2.pending) # Locally pending now
        
        t1 = time.time()
        pending = self.coherence.query_pending(1, lambda:1, optimistic_lock = True)
        t2 = time.time()
        
        self.assertNotEqual(pending, None) # pending on coherence2
        self.assertLess(t2-t1, 1.0) # No timeout
        self.assertTrue(1 not in self.coherence.pending) # Not locally pending

        def mark():
            time.sleep(0.1)
            self.coherence2.mark_done(1)
        t = threading.Thread(target=mark)
        t.deamon = True
        t.start()
        self.coherence.wait_done(1)

        time.sleep(0.1)

        t1 = time.time()
        pending = self.coherence.query_pending(1, lambda:1, optimistic_lock = True)
        t2 = time.time()
        
        self.assertEqual(pending, None) # Not pending anymore on coherence2
        self.assertLess(t2-t1, 1.0) # No timeout
        self.assertTrue(1 in self.coherence.pending) # Locally pending

    def test_pendq_lock_r(self):
        t1 = time.time()
        pending = self.coherence.query_pending(1, lambda:1, optimistic_lock = True)
        t2 = time.time()
        
        self.assertEqual(pending, None) # No pending
        self.assertLess(t2-t1, 1.0) # No timeout
        self.assertTrue(1 in self.coherence.pending) # Locally pending now
        
        t1 = time.time()
        pending = self.coherence2.query_pending(1, lambda:1, optimistic_lock = True)
        t2 = time.time()
        
        self.assertNotEqual(pending, None) # pending on coherence
        self.assertLess(t2-t1, 1.0) # No timeout
        self.assertTrue(1 not in self.coherence2.pending) # Not locally pending

        def mark():
            time.sleep(0.1)
            self.coherence.mark_done(1)
        t = threading.Thread(target=mark)
        t.deamon = True
        t.start()
        self.coherence2.wait_done(1)

        t1 = time.time()
        pending = self.coherence2.query_pending(1, lambda:1, optimistic_lock = True)
        t2 = time.time()
        
        self.assertEqual(pending, None) # Not pending anymore on coherence2
        self.assertLess(t2-t1, 1.0) # No timeout
        self.assertTrue(1 in self.coherence2.pending) # Locally pending

    def test_wait_on_dead(self):
        t1 = time.time()
        pending = self.coherence2.query_pending(1, lambda:1, optimistic_lock = True)
        t2 = time.time()
        
        self.assertEqual(pending, None) # No pending
        self.assertLess(t2-t1, 1.0) # No timeout
        self.assertTrue(1 in self.coherence2.pending) # Locally pending now

        t1 = time.time()
        pending = self.coherence.query_pending(1, lambda:1, optimistic_lock = True)
        t2 = time.time()
        
        self.assertNotEqual(pending, None) # pending on coherence2
        self.assertLess(t2-t1, 1.0) # No timeout
        self.assertTrue(1 not in self.coherence.pending) # Not locally pending

        # Coherence2 dies... coherence must pick up
        self.ipsub2.terminate()
        self.ipsub2.wake()
        self.ipsub2_thread.join(5.0)

        t1 = time.time()
        waiter = threading.Thread(target=self.coherence.wait_done, args=(1,))
        waiter.daemon = True
        waiter.start()
        waiter.join(3*coherence.PENDING_TIMEOUT)
        t2 = time.time()

        self.assertLess(t2-t1, 2*coherence.PENDING_TIMEOUT) # No excessive timeout

    def test_wait_on_aborted(self):
        t1 = time.time()
        pending = self.coherence2.query_pending(1, lambda:1, optimistic_lock = True)
        t2 = time.time()
        
        self.assertEqual(pending, None) # No pending
        self.assertLess(t2-t1, 1.0) # No timeout
        self.assertTrue(1 in self.coherence2.pending) # Locally pending now
        
        t1 = time.time()
        pending = self.coherence.query_pending(1, lambda:1, optimistic_lock = True)
        t2 = time.time()
        
        self.assertNotEqual(pending, None) # pending on coherence2
        self.assertLess(t2-t1, 1.0) # No timeout
        self.assertTrue(1 not in self.coherence.pending) # Not locally pending

        t1 = time.time()
        waiter = threading.Thread(target=self.coherence.wait_done, args=(1,))
        waiter.daemon = True
        waiter.start()

        # Coherence2 aborts... coherence must pick up
        time.sleep(0.1)
        self.coherence2.mark_aborted(1)
        
        waiter.join(3*coherence.PENDING_TIMEOUT)
        t2 = time.time()

        self.assertLess(t2-t1, 2*coherence.PENDING_TIMEOUT) # No excessive timeout

    def test_wait_on_undead(self):
        t1 = time.time()
        pending = self.coherence2.query_pending(1, lambda:1, optimistic_lock = True)
        t2 = time.time()
        
        self.assertEqual(pending, None) # No pending
        self.assertLess(t2-t1, 1.0) # No timeout
        self.assertTrue(1 in self.coherence2.pending) # Locally pending now
        
        t1 = time.time()
        pending = self.coherence.query_pending(1, lambda:1, optimistic_lock = True)
        t2 = time.time()
        
        self.assertNotEqual(pending, None) # pending on coherence2
        self.assertLess(t2-t1, 1.0) # No timeout
        self.assertTrue(1 not in self.coherence.pending) # Not locally pending

        t1 = time.time()
        waiter = threading.Thread(target=self.coherence.wait_done, args=(1,))
        waiter.daemon = True
        waiter.start()
        waiter.join(3*coherence.PENDING_TIMEOUT)
        t2 = time.time()
        self.coherence2.mark_done(1)

        self.assertGreater(t2-t1, 2*coherence.PENDING_TIMEOUT) # Didn't finish before the timeout

    def test_pendq_oob(self):
        t1 = time.time()
        pending = self.coherence.query_pending(1, lambda:0)
        t2 = time.time()
        
        self.assertNotEqual(pending, None) # IB or OOB update
        self.assertLess(t2-t1, 1.0) # No timeout
        
        t1 = time.time()
        pending = self.coherence2.query_pending(1, lambda:0)
        t2 = time.time()
        
        self.assertNotEqual(pending, None) # IB or OOB update
        self.assertLess(t2-t1, 1.0) # No timeout

    def test_disconnected(self):
        t1 = time.time()
        pending = self.coherence3.query_pending(1, lambda:1)
        t2 = time.time()
        
        self.assertEqual(pending, None) # No pending
        self.assertLess(t2-t1, 1.0) # No timeout

        # No crashing
        pending = self.coherence3.fire_deletion(1)
        

@skipIfUnsupported
class CoherenceQuickProtocolTest(CoherenceProtocolTest):
    coherence_kwargs = {'quick_refresh':True}


