package org.dancres.peers.ring;

import com.ning.http.client.AsyncHttpClient;
import org.dancres.peers.primitives.*;
import org.junit.Assert;
import org.dancres.peers.Directory;
import org.dancres.peers.Peer;
import org.dancres.peers.PeerSet;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsistentHashTest {
    private static final Logger _logger = LoggerFactory.getLogger(ConsistentHashTest.class);

    @Test
    public void testListenerReject() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8081));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());
        Peer myPeer2 = new InProcessPeer(myServer, myClient, "/peer2", new Timer());

        Set<URI> myPeers = new HashSet<>();
        myPeers.add(myPeer1.getURI());
        myPeers.add(myPeer2.getURI());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet, 500, 3000);
        Directory myPeer2Dir = new Directory(myPeer2, myPeerSet, 500, 3000);

        GossipBarrier myBarrier1 = new GossipBarrier(myPeer1Dir);
        GossipBarrier myBarrier2 = new GossipBarrier(myPeer2Dir);

        int myBarr1Curr = myBarrier1.current();
        int myBarr2Curr = myBarrier2.current();

        myPeer1Dir.start();

        myBarrier1.await(myBarr1Curr);
        myBarrier2.await(myBarr2Curr);

        Assert.assertEquals(2, myPeer1Dir.getDirectory().size());
        Assert.assertEquals(2, myPeer2Dir.getDirectory().size());

        AtomicInteger myPeer1RejectCount = new AtomicInteger(0);
        AtomicInteger myPeer2RejectCount = new AtomicInteger(0);

        ConsistentHash<Integer> myRing1 = ConsistentHash.createRing(myPeer1);
        ConsistentHash<Integer> myRing2 = ConsistentHash.createRing(myPeer2);

        myRing1.add(new RejectionCountingListenerImpl(myPeer1RejectCount));
        myRing2.add(new RejectionCountingListenerImpl(myPeer2RejectCount));

        myRing1.createPosition(1);

        try {
            Thread.sleep(50);
        } catch (Exception anE) {
            myPeer1Dir.walk(new LoggerWriter(_logger));
            myPeer2Dir.walk(new LoggerWriter(_logger));

            Assert.fail();
        }

        myRing2.createPosition(1);

        // Allow some gossip time so that these ring positions have "taken" across the cluster of peers
        // Ring 2 contains a conflicting, newer position which when propagated should cause collisions in peer1
        // and peer2. Peer1 should be silent, Peer2 should complain
        //
        myBarrier1.await(myBarrier1.current());
        myBarrier2.await(myBarrier2.current());
        myBarrier1.await(myBarrier1.current());
        myBarrier2.await(myBarrier2.current());

        myPeer1Dir.walk(new LoggerWriter(_logger));
        myPeer2Dir.walk(new LoggerWriter(_logger));

        Assert.assertEquals(0, myPeer1RejectCount.get());
        Assert.assertEquals(1, myPeer2RejectCount.get());

        Assert.assertEquals(1, myRing1.getPeerPositions().getPositions().size());
        Assert.assertEquals(0, myRing2.getPeerPositions().getPositions().size());

        Assert.assertEquals(1, myRing1.getRing().getPositions().size());
        Assert.assertEquals(1, myRing2.getRing().getPositions().size());

        myPeer1.stop();
        myPeer2.stop();

        myServer.terminate();
    }

    class RejectionCountingListenerImpl implements ConsistentHash.Listener {
        private AtomicInteger _count;

        RejectionCountingListenerImpl(AtomicInteger aCount) {
            _count = aCount;
        }

        public void newNeighbour(ConsistentHash aRing, RingPosition anOwnedPosition,
                                 RingPosition aNeighbourPosition) {
        }

        public void rejected(ConsistentHash aRing, RingPosition anOwnedPosition) {
            _count.incrementAndGet();
        }
    }

    @Test
    public void testGetsStable() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8083));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());
        Peer myPeer2 = new InProcessPeer(myServer, myClient, "/peer2", new Timer());

        Set<URI> myPeers = new HashSet<>();
        myPeers.add(myPeer1.getURI());
        myPeers.add(myPeer2.getURI());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet, 500, 3000);
        Directory myPeer2Dir = new Directory(myPeer2, myPeerSet, 500, 3000);

        GossipBarrier myBarrier1 = new GossipBarrier(myPeer1Dir);
        GossipBarrier myBarrier2 = new GossipBarrier(myPeer2Dir);

        int myBarr1Curr = myBarrier1.current();
        int myBarr2Curr = myBarrier2.current();

        myPeer1Dir.start();

        myBarrier1.await(myBarr1Curr);
        myBarrier2.await(myBarr2Curr);

        myPeer1Dir.walk(new LoggerWriter(_logger));
        myPeer2Dir.walk(new LoggerWriter(_logger));

        // Directory is stable?
        //
        Assert.assertEquals(2, myPeer1Dir.getDirectory().size());
        Assert.assertEquals(2, myPeer2Dir.getDirectory().size());

        ConsistentHash<Integer> myRing1 = ConsistentHash.createRing(myPeer1);
        ConsistentHash<Integer> myRing2 = ConsistentHash.createRing(myPeer2);

        myRing1.add(new StabiliserImpl());
        myRing2.add(new StabiliserImpl());

        for (int i = 0; i < 3; i++) {
            myRing1.createPosition();
            myRing2.createPosition();
        }

        // Allow some gossip time so that this ring position has "taken" across the cluster of peers
        //
        for (int i = 0; i < 10; i++) {
            myBarrier1.await(myBarrier1.current());
            myBarrier2.await(myBarrier2.current());

            if ((myRing1.getRing().getPositions().size() == 6) && (myRing2.getRing().getPositions().size() == 6))
                break;
        }

        myPeer1Dir.walk(new LoggerWriter(_logger));
        myPeer2Dir.walk(new LoggerWriter(_logger));

        Assert.assertEquals(6, myRing1.getRing().getPositions().size());
        Assert.assertEquals(6, myRing2.getRing().getPositions().size());

        myPeer1.stop();
        myPeer2.stop();

        myServer.terminate();
    }

    @Test
    public void testIterators() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8084));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());
        Peer myPeer2 = new InProcessPeer(myServer, myClient, "/peer2", new Timer());

        Set<URI> myPeers = new HashSet<>();
        myPeers.add(myPeer1.getURI());
        myPeers.add(myPeer2.getURI());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet, 500, 3000);
        Directory myPeer2Dir = new Directory(myPeer2, myPeerSet, 500, 3000);

        GossipBarrier myBarrier1 = new GossipBarrier(myPeer1Dir);
        GossipBarrier myBarrier2 = new GossipBarrier(myPeer2Dir);

        int myBarr1Curr = myBarrier1.current();
        int myBarr2Curr = myBarrier2.current();

        myPeer1Dir.start();

        myBarrier1.await(myBarr1Curr);
        myBarrier2.await(myBarr2Curr);

        myPeer1Dir.walk(new LoggerWriter(_logger));
        myPeer2Dir.walk(new LoggerWriter(_logger));

        ConsistentHash<Integer> myRing1 = ConsistentHash.createRing(myPeer1);
        ConsistentHash<Integer> myRing2 = ConsistentHash.createRing(myPeer2);

        myRing1.add(new StabiliserImpl());
        myRing2.add(new StabiliserImpl());

        for (int i = 0; i < 3; i++) {
            myRing1.createPosition();
            myRing2.createPosition();
        }

        // Allow some gossip time so that this ring position has "taken" across the cluster of peers
        //
        boolean myAllPresent = false;

        for (int i = 0; i < 10; i++) {
            myBarrier1.await(myBarrier1.current());
            myBarrier2.await(myBarrier2.current());

            if ((myRing1.getRing().getPositions().size() == 6) && (myRing2.getRing().getPositions().size() == 6)) {
                myAllPresent = true;
                break;
            }
        }

        Assert.assertTrue(myAllPresent);

        int myTotal = 0;
        Integer myPrevPosn = myRing1.getRing().iterator().next().getPosition();

        for (RingPosition<Integer> myPosn : myRing1.getRing()) {
            _logger.debug("ComparingFwd " + myPrevPosn + " with " + myPosn.getPosition() + " = " +
                myPrevPosn.compareTo(myPosn.getPosition()) + " so " +
                                (myPrevPosn.compareTo(myPosn.getPosition()) <= 0));

            Assert.assertTrue((myPrevPosn.compareTo(myPosn.getPosition()) <= 0));

            myTotal++;

            if ((myTotal % 6) == 0)
                myPrevPosn = myRing1.getRing().iterator().next().getPosition();
            else
                myPrevPosn = myPosn.getPosition();

            if (myTotal > 11)
                break;
        }

        myTotal = 0;
        myPrevPosn = myRing1.getRing().reverseIterator().next().getPosition();
        Iterator<RingPosition<Integer>> mySource = myRing1.getRing().reverseIterator();

        while (true) {
            RingPosition<Integer> myPosn = mySource.next();

            _logger.debug("ComparingRev " + myPrevPosn + " with " + myPosn.getPosition() + " = " +
                    myPrevPosn.compareTo(myPosn.getPosition()) + " so " +
                    (myPrevPosn.compareTo(myPosn.getPosition()) >= 0));

            Assert.assertTrue((myPrevPosn.compareTo(myPosn.getPosition()) >= 0));

            myTotal++;

            if ((myTotal % 6) == 0)
                myPrevPosn = myRing1.getRing().reverseIterator().next().getPosition();
            else
                myPrevPosn = myPosn.getPosition();

            if (myTotal > 11)
                break;
        }

        myPeer1.stop();
        myPeer2.stop();

        myServer.terminate();
    }

    @Test
    public void collisionDetection() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8085));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());
        Peer myPeer2 = new InProcessPeer(myServer, myClient, "/peer2", new Timer());

        Set<URI> myPeers = new HashSet<>();
        myPeers.add(myPeer1.getURI());
        myPeers.add(myPeer2.getURI());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet, 500, 3000);
        Directory myPeer2Dir = new Directory(myPeer2, myPeerSet, 500, 3000);

        GossipBarrier myBarrier1 = new GossipBarrier(myPeer1Dir);
        GossipBarrier myBarrier2 = new GossipBarrier(myPeer2Dir);

        int myBarr1Curr = myBarrier1.current();
        int myBarr2Curr = myBarrier2.current();

        myPeer1Dir.start();

        myBarrier1.await(myBarr1Curr);
        myBarrier2.await(myBarr2Curr);

        myPeer1Dir.walk(new LoggerWriter(_logger));
        myPeer2Dir.walk(new LoggerWriter(_logger));

        Assert.assertEquals(2, myPeer1Dir.getDirectory().size());
        Assert.assertEquals(2, myPeer2Dir.getDirectory().size());

        ConsistentHash<Integer> myRing1 = ConsistentHash.createRing(myPeer1);
        ConsistentHash<Integer> myRing2 = ConsistentHash.createRing(myPeer2);

        myRing1.createPosition(1);

        // Allow some gossip time so that this ring position has "taken" across the cluster of peers
        //
        myBarrier1.await(myBarrier1.current());
        myBarrier2.await(myBarrier2.current());

        try {
            myRing2.createPosition(1);

            myPeer1Dir.walk(new LoggerWriter(_logger));
            myPeer2Dir.walk(new LoggerWriter(_logger));

            Assert.fail();
        } catch (CollisionException aCE) {
            // Should get an exception
        }

        myPeer1.stop();
        myPeer2.stop();

        myServer.terminate();
    }

    @Test
    public void testGetsStableWithName() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8086));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());
        Peer myPeer2 = new InProcessPeer(myServer, myClient, "/peer2", new Timer());

        Set<URI> myPeers = new HashSet<>();
        myPeers.add(myPeer1.getURI());
        myPeers.add(myPeer2.getURI());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet, 500, 3000);
        Directory myPeer2Dir = new Directory(myPeer2, myPeerSet, 500, 3000);

        GossipBarrier myBarrier1 = new GossipBarrier(myPeer1Dir);
        GossipBarrier myBarrier2 = new GossipBarrier(myPeer2Dir);

        int myBarr1Curr = myBarrier1.current();
        int myBarr2Curr = myBarrier2.current();

        myPeer1Dir.start();

        myBarrier1.await(myBarr1Curr);
        myBarrier2.await(myBarr2Curr);

        myPeer1Dir.walk(new LoggerWriter(_logger));
        myPeer2Dir.walk(new LoggerWriter(_logger));

        // Directory is stable?
        //
        Assert.assertEquals(2, myPeer1Dir.getDirectory().size());
        Assert.assertEquals(2, myPeer2Dir.getDirectory().size());

        ConsistentHash<Integer> myRing1 = ConsistentHash.createRing(myPeer1);
        ConsistentHash<Integer> myRing2 = ConsistentHash.createRing(myPeer2);

        myRing1.add(new StabiliserImpl());
        myRing2.add(new StabiliserImpl());

        for (int i = 0; i < 3; i++) {
            myRing1.createPosition();
            myRing2.createPosition();
        }

        // Allow some gossip time so that this ring position has "taken" across the cluster of peers
        //
        for (int i = 0; i < 10; i++) {
            myBarrier1.await(myBarrier1.current());
            myBarrier2.await(myBarrier2.current());

            if ((myRing1.getRing().getPositions().size() == 6) && (myRing2.getRing().getPositions().size() == 6))
                break;
        }

        myPeer1Dir.walk(new LoggerWriter(_logger));
        myPeer2Dir.walk(new LoggerWriter(_logger));

        Assert.assertEquals(6, myRing1.getRing().getPositions().size());
        Assert.assertEquals(6, myRing2.getRing().getPositions().size());

        myPeer1.stop();
        myPeer2.stop();

        myServer.terminate();
    }

    @Test
    public void testAllocation() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8087));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());
        Peer myPeer2 = new InProcessPeer(myServer, myClient, "/peer2", new Timer());

        Set<URI> myPeers = new HashSet<>();
        myPeers.add(myPeer1.getURI());
        myPeers.add(myPeer2.getURI());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet, 500, 3000);
        Directory myPeer2Dir = new Directory(myPeer2, myPeerSet, 500, 3000);

        GossipBarrier myBarrier1 = new GossipBarrier(myPeer1Dir);
        GossipBarrier myBarrier2 = new GossipBarrier(myPeer2Dir);

        int myBarr1Curr = myBarrier1.current();
        int myBarr2Curr = myBarrier2.current();

        myPeer1Dir.start();

        myBarrier1.await(myBarr1Curr);
        myBarrier2.await(myBarr2Curr);

        ConsistentHash<Integer> myRing1 = ConsistentHash.createRing(myPeer1);
        ConsistentHash<Integer> myRing2 = ConsistentHash.createRing(myPeer2);

        myRing1.add(new StabiliserImpl());
        myRing2.add(new StabiliserImpl());

        for (int i = 0; i < 3; i++) {
            myRing1.createPosition();
            myRing2.createPosition();
        }

        // Allow some gossip time so that this ring position has "taken" across the cluster of peers
        //
        for (int i = 0; i < 10; i++) {
            myBarrier1.await(myBarrier1.current());
            myBarrier2.await(myBarrier2.current());

            if ((myRing1.getRing().getPositions().size() == 6) && (myRing2.getRing().getPositions().size() == 6))
                break;
        }

        Assert.assertEquals(6, myRing1.getRing().getPositions().size());
        Assert.assertEquals(6, myRing2.getRing().getPositions().size());

        // Ring is stable, we can proceed....
        SortedSet<RingPosition<Integer>> myRing = myRing1.getRing().getPositions();

        try {
            // Should mean we grab some number of positions from first
            //
            int myHash1 = ((int) myRing.first().getPosition()) - 1;

            // Should mean we grab some number of positions from last
            //
            int myHash2 = ((int) myRing.last().getPosition()) - 1;

            Collection<RingPosition<Integer>> myResults = myRing1.getRing().allocate(myHash1, 3);

            // Test for size and uniqueness
            //
            Assert.assertEquals(3, myResults.size());
            Assert.assertEquals(3, new HashSet<>(myResults).size());

            myResults = myRing1.getRing().allocate(myHash2, 3);

            Assert.assertEquals(3, myResults.size());
            Assert.assertEquals(3, new HashSet<>(myResults).size());

        } catch (Throwable aT) {
            Assert.fail();
        }

        myPeer1.stop();
        myPeer2.stop();

        myServer.terminate();
    }
}
