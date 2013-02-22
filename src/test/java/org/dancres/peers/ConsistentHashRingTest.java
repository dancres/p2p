package org.dancres.peers;

import com.ning.http.client.AsyncHttpClient;
import junit.framework.Assert;
import org.dancres.peers.primitives.HttpServer;
import org.dancres.peers.primitives.InProcessPeer;
import org.dancres.peers.primitives.StaticPeerSet;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsistentHashRingTest {
    @Test
    public void testListener() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8081));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());
        Peer myPeer2 = new InProcessPeer(myServer, myClient, "/peer2", new Timer());

        Set<URI> myPeers = new HashSet<URI>();
        myPeers.add(myPeer1.getURI());
        myPeers.add(myPeer2.getURI());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet);
        Directory myPeer2Dir = new Directory(myPeer2, myPeerSet);

        myPeer1Dir.start();

        Thread.sleep(1000);

        Assert.assertEquals(2, myPeer1Dir.getDirectory().size());
        Assert.assertEquals(2, myPeer2Dir.getDirectory().size());

        ConsistentHashRing myRing1 = new ConsistentHashRing(myPeer1, myPeer1Dir);
        ConsistentHashRing myRing2 = new ConsistentHashRing(myPeer2, myPeer2Dir);

        myRing1.insertPosition(new ConsistentHashRing.RingPosition(myPeer1, 1, System.currentTimeMillis()));

        // Allow some gossip time so that this ring position has "taken" across the cluster of peers
        //
        Thread.sleep(10000);

        AtomicInteger myPeer1RejectCount = new AtomicInteger(0);
        AtomicInteger myPeer2RejectCount = new AtomicInteger(0);

        myRing1.add(new ListenerImpl(myPeer1RejectCount));
        myRing2.add(new ListenerImpl(myPeer2RejectCount));
        myRing2.insertPosition(new ConsistentHashRing.RingPosition(myPeer2, 1, System.currentTimeMillis()));

        // Ring 2 contains a conflicting, newer position which when propagated should cause collisions in peer1
        // and peer2. Peer1 should be silent, Peer2 should complain
        //
        Thread.sleep(10000);

        Assert.assertEquals(0, myPeer1RejectCount.get());
        Assert.assertEquals(1, myPeer2RejectCount.get());

        Assert.assertEquals(1, myRing1.getCurrentPositions().getPositions().size());
        Assert.assertEquals(0, myRing2.getCurrentPositions().getPositions().size());

        Assert.assertEquals(1, myRing1.getCurrentRing().size());
        Assert.assertEquals(1, myRing2.getCurrentRing().size());
    }

    class ListenerImpl implements ConsistentHashRing.Listener {
        private AtomicInteger _count;

        ListenerImpl(AtomicInteger aCount) {
            _count = aCount;
        }

        public void newNeighbour(ConsistentHashRing.RingPosition anOwnedPosition,
                                 ConsistentHashRing.RingPosition aNeighbourPosition) {
        }

        public void rejected(ConsistentHashRing.RingPosition anOwnedPosition) {
            _count.incrementAndGet();
        }
    }
}
