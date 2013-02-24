package org.dancres.peers;

import com.ning.http.client.AsyncHttpClient;
import junit.framework.Assert;
import org.dancres.peers.primitives.GossipBarrier;
import org.dancres.peers.primitives.HttpServer;
import org.dancres.peers.primitives.InProcessPeer;
import org.dancres.peers.primitives.StaticPeerSet;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsistentHashRingTest {
    @Test
    public void testListenerReject() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8081));
        AsyncHttpClient myClient = new AsyncHttpClient();
        GossipBarrier myBarrier1 = new GossipBarrier();
        GossipBarrier myBarrier2 = new GossipBarrier();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());
        Peer myPeer2 = new InProcessPeer(myServer, myClient, "/peer2", new Timer());

        Set<URI> myPeers = new HashSet<URI>();
        myPeers.add(myPeer1.getURI());
        myPeers.add(myPeer2.getURI());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet);
        Directory myPeer2Dir = new Directory(myPeer2, myPeerSet);

        myPeer1Dir.add(myBarrier1);
        myPeer2Dir.add(myBarrier2);

        int myBarr1Curr = myBarrier1.current();
        int myBarr2Curr = myBarrier2.current();

        myPeer1Dir.start();

        myBarrier1.await(myBarr1Curr);
        myBarrier2.await(myBarr2Curr);

        Assert.assertEquals(2, myPeer1Dir.getDirectory().size());
        Assert.assertEquals(2, myPeer2Dir.getDirectory().size());

        ConsistentHashRing myRing1 = new ConsistentHashRing(myPeer1, myPeer1Dir);
        ConsistentHashRing myRing2 = new ConsistentHashRing(myPeer2, myPeer2Dir);

        myRing1.insertPosition(new ConsistentHashRing.RingPosition(myPeer1, 1, System.currentTimeMillis()));

        // Allow some gossip time so that this ring position has "taken" across the cluster of peers
        //
        myBarr1Curr = myBarrier1.current();
        myBarr2Curr = myBarrier2.current();

        myBarrier1.await(myBarr1Curr);
        myBarrier2.await(myBarr2Curr);

        AtomicInteger myPeer1RejectCount = new AtomicInteger(0);
        AtomicInteger myPeer2RejectCount = new AtomicInteger(0);

        myRing1.add(new RejectionCountingListenerImpl(myPeer1RejectCount));
        myRing2.add(new RejectionCountingListenerImpl(myPeer2RejectCount));
        myRing2.insertPosition(new ConsistentHashRing.RingPosition(myPeer2, 1, System.currentTimeMillis()));

        // Ring 2 contains a conflicting, newer position which when propagated should cause collisions in peer1
        // and peer2. Peer1 should be silent, Peer2 should complain
        //
        myBarr1Curr = myBarrier1.current();
        myBarr2Curr = myBarrier2.current();

        myBarrier1.await(myBarr1Curr);
        myBarrier2.await(myBarr2Curr);

        Assert.assertEquals(0, myPeer1RejectCount.get());
        Assert.assertEquals(1, myPeer2RejectCount.get());

        Assert.assertEquals(1, myRing1.getCurrentPositions().getPositions().size());
        Assert.assertEquals(0, myRing2.getCurrentPositions().getPositions().size());

        Assert.assertEquals(1, myRing1.getCurrentRing().size());
        Assert.assertEquals(1, myRing2.getCurrentRing().size());

        myPeer1.stop();
        myPeer2.stop();
    }

    class RejectionCountingListenerImpl implements ConsistentHashRing.Listener {
        private AtomicInteger _count;

        RejectionCountingListenerImpl(AtomicInteger aCount) {
            _count = aCount;
        }

        public void newNeighbour(ConsistentHashRing.RingPosition anOwnedPosition,
                                 ConsistentHashRing.RingPosition aNeighbourPosition) {
        }

        public void rejected(ConsistentHashRing.RingPosition anOwnedPosition) {
            _count.incrementAndGet();
        }
    }

    @Test
    public void testListenerNeighbour() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8082));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());
        Peer myPeer2 = new InProcessPeer(myServer, myClient, "/peer2", new Timer());
        GossipBarrier myBarrier1 = new GossipBarrier();
        GossipBarrier myBarrier2 = new GossipBarrier();

        Set<URI> myPeers = new HashSet<URI>();
        myPeers.add(myPeer1.getURI());
        myPeers.add(myPeer2.getURI());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet);
        Directory myPeer2Dir = new Directory(myPeer2, myPeerSet);

        myPeer1Dir.add(myBarrier1);
        myPeer2Dir.add(myBarrier2);

        int myBarr1Curr = myBarrier1.current();
        int myBarr2Curr = myBarrier2.current();

        myPeer1Dir.start();

        myBarrier1.await(myBarr1Curr);
        myBarrier2.await(myBarr2Curr);

        Assert.assertEquals(2, myPeer1Dir.getDirectory().size());
        Assert.assertEquals(2, myPeer2Dir.getDirectory().size());

        ConsistentHashRing myRing1 = new ConsistentHashRing(myPeer1, myPeer1Dir);
        ConsistentHashRing myRing2 = new ConsistentHashRing(myPeer2, myPeer2Dir);

        AtomicInteger myPeer1NeighbourCount = new AtomicInteger(0);
        AtomicInteger myPeer2NeighbourCount = new AtomicInteger(0);

        myRing1.add(new NeighbourCountingListenerImpl(myPeer1NeighbourCount));
        myRing2.add(new NeighbourCountingListenerImpl(myPeer2NeighbourCount));

        myRing1.insertPosition(new ConsistentHashRing.RingPosition(myPeer1, 1, System.currentTimeMillis()));
        myRing2.insertPosition(new ConsistentHashRing.RingPosition(myPeer2, 3, System.currentTimeMillis()));

        // Allow some gossip time so that this ring position has "taken" across the cluster of peers
        // Have to wait a couple of cycles because neighbour processing might lag a little behind our discovery
        // events
        //
        myBarrier1.await(myBarrier1.current());
        myBarrier2.await(myBarrier2.current());
        myBarrier1.await(myBarrier1.current());
        myBarrier2.await(myBarrier2.current());

        Assert.assertEquals(1, myPeer1NeighbourCount.get());
        Assert.assertEquals(1, myPeer2NeighbourCount.get());

        Assert.assertEquals(2, myRing1.getCurrentRing().size());
        Assert.assertEquals(2, myRing2.getCurrentRing().size());

        Set<ConsistentHashRing.NeighbourRelation> myRels = myRing1.getNeighbours();

        Assert.assertEquals(1, myRels.size());

        ConsistentHashRing.NeighbourRelation myRel = myRels.iterator().next();

        // In ring 1, owns position 1, neighbour should be 3
        //
        Assert.assertEquals(1, myRel.getOwned().getPosition().intValue());
        Assert.assertEquals(3, myRel.getNeighbour().getPosition().intValue());

        myRels = myRing2.getNeighbours();

        Assert.assertEquals(1, myRels.size());

        myRel = myRels.iterator().next();

        // In ring 2, owns position 3, neighbour should be 1
        //
        Assert.assertEquals(3, myRel.getOwned().getPosition().intValue());
        Assert.assertEquals(1, myRel.getNeighbour().getPosition().intValue());

        myPeer1.stop();
        myPeer2.stop();
    }

    class NeighbourCountingListenerImpl implements ConsistentHashRing.Listener {
        private AtomicInteger _count;

        NeighbourCountingListenerImpl(AtomicInteger aCount) {
            _count = aCount;
        }

        public void newNeighbour(ConsistentHashRing.RingPosition anOwnedPosition,
                                 ConsistentHashRing.RingPosition aNeighbourPosition) {
            _count.incrementAndGet();
        }

        public void rejected(ConsistentHashRing.RingPosition anOwnedPosition) {
        }
    }
}
