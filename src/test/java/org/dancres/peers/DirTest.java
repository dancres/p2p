package org.dancres.peers;

import com.ning.http.client.AsyncHttpClient;
import org.junit.Assert;
import org.dancres.peers.primitives.GossipBarrier;
import org.dancres.peers.primitives.HttpServer;
import org.dancres.peers.primitives.InProcessPeer;
import org.dancres.peers.primitives.StaticPeerSet;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DirTest {
    private Logger _logger = LoggerFactory.getLogger(DirTest.class);

    @Test
    public void testGossip() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8081));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());
        Peer myPeer2 = new InProcessPeer(myServer, myClient, "/peer2", new Timer());

        Set<URI> myPeers = new HashSet<URI>();
        myPeers.add(myPeer1.getURI());
        myPeers.add(myPeer2.getURI());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet, 2000, 12000);
        Directory myPeer2Dir = new Directory(myPeer2, myPeerSet, 2000, 12000);
        GossipBarrier myBarrier1 = new GossipBarrier(myPeer1Dir);
        GossipBarrier myBarrier2 = new GossipBarrier(myPeer2Dir);

        // We want some skew between birth time of directory and gossip time to test timestamp/liveness
        //
        Thread.sleep(1000);

        int myBarr1 = myBarrier1.current();
        int myBarr2 = myBarrier1.current();

        myPeer1Dir.start();

        myBarrier1.await(myBarr1);
        myBarrier1.await(myBarr2);

        Assert.assertEquals(2, myPeer1Dir.getDirectory().size());
        Assert.assertEquals(2, myPeer2Dir.getDirectory().size());

        Assert.assertTrue(myPeer1Dir.getDirectory().containsKey(myPeer1.getAddress()));
        Assert.assertTrue(myPeer1Dir.getDirectory().containsKey(myPeer2.getAddress()));

        Assert.assertTrue(myPeer2Dir.getDirectory().containsKey(myPeer1.getAddress()));
        Assert.assertTrue(myPeer2Dir.getDirectory().containsKey(myPeer2.getAddress()));

        Directory.Entry myPeer1Entry = myPeer1Dir.getDirectory().get(myPeer1.getAddress());

        Assert.assertNotSame(myPeer1Entry.getBorn(), myPeer1Entry.getTimestamp());

        Directory.Entry myPeer2Entry = myPeer2Dir.getDirectory().get(myPeer2.getAddress());

        Assert.assertNotSame(myPeer2Entry.getBorn(), myPeer2Entry.getTimestamp());

        myServer.terminate();
    }

    @Test
    public void testAttributes() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8082));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());

        Set<URI> myPeers = new HashSet<URI>();
        myPeers.add(myPeer1.getURI());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet, 2000, 12000);

        myPeer1Dir.add(new Directory.AttributeProducer() {
            public Map<String, String> produce() {
                Map<String, String> myAttrs = new HashMap<String, String>();

                myAttrs.put("testAttr", "testValue");
                return myAttrs;
            }
        });

        Map<String, String> myAttrs = myPeer1Dir.getAttributes();

        Assert.assertNotNull(myAttrs);
        Assert.assertNotNull(myAttrs.get("testAttr"));
        Assert.assertEquals("testValue", myAttrs.get("testAttr"));

        myServer.terminate();
    }

    @Test
    public void testListener() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8083));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());
        Peer myPeer2 = new InProcessPeer(myServer, myClient, "/peer2", new Timer());

        Set<URI> myPeers = new HashSet<URI>();
        myPeers.add(myPeer1.getURI());
        myPeers.add(myPeer2.getURI());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet, 2000, 6000);
        Directory myPeer2Dir = new Directory(myPeer2, myPeerSet, 2000, 6000);
        final AtomicInteger myEventCount = new AtomicInteger(0);
        final AtomicInteger myDeadCount = new AtomicInteger(0);

        GossipBarrier myBarrier1 = new GossipBarrier(myPeer1Dir);
        GossipBarrier myBarrier2 = new GossipBarrier(myPeer2Dir);

        myPeer1Dir.add(new Directory.Listener() {
            public void updated(Directory aDirectory, List<Directory.Entry> aNewPeers,
                                List<Directory.Entry> anUpdatedPeers, List<Directory.Entry> aDeadPeers) {
                _logger.info("Listener update: " + aNewPeers.size() + ", " + anUpdatedPeers.size() + ", " +
                    aDeadPeers.size());

                if (aDeadPeers.size() > 0)
                    myDeadCount.incrementAndGet();

                myEventCount.incrementAndGet();
            }
        });

        int myBarr1;
        int myBarr2;

        myPeer1Dir.start();

        for (int i = 0; i < 2; i++) {
            myBarr1 = myBarrier1.current();
            myBarr2 = myBarrier2.current();

            myBarrier1.await(myBarr1);
            myBarrier2.await(myBarr2);
        }

        Assert.assertEquals(2, myPeer1Dir.getDirectory().size());
        Assert.assertEquals(2, myPeer2Dir.getDirectory().size());

        Assert.assertEquals(1, myEventCount.get());

        // Stop a peer and make sure we find out about it
        //
        myPeer2.stop();

        _logger.info("Peer 2 has been stopped @ " + System.currentTimeMillis());

        for (int i = 0; i < 4; i++) {
            myBarr1 = myBarrier1.current();
            myBarrier1.await(myBarr1);
        }

        Assert.assertEquals(1, myPeer1Dir.getDirectory().size());
        Assert.assertEquals(1, myDeadCount.get());

        myServer.terminate();
    }
}
