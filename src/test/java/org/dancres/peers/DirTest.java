package org.dancres.peers;

import com.ning.http.client.AsyncHttpClient;
import junit.framework.Assert;
import org.dancres.peers.primitives.HttpServer;
import org.dancres.peers.primitives.InProcessPeer;
import org.dancres.peers.primitives.StaticPeerSet;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DirTest {
    @Test
    public void testGossip() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8081));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());
        Peer myPeer2 = new InProcessPeer(myServer, myClient, "/peer2", new Timer());

        Set<URI> myPeers = new HashSet<URI>();
        myPeers.add(myPeer1.getAddress());
        myPeers.add(myPeer2.getAddress());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet);
        Directory myPeer2Dir = new Directory(myPeer2, myPeerSet);

        // We want some skew between birth time of directory and gossip time to test timestamp/liveness
        //
        Thread.sleep(1000);

        myPeer1Dir.start();

        Thread.sleep(1000);

        Assert.assertEquals(2, myPeer1Dir.getDirectory().size());
        Assert.assertEquals(2, myPeer2Dir.getDirectory().size());

        Assert.assertTrue(myPeer1Dir.getDirectory().containsKey(myPeer1.getAddress().toString()));
        Assert.assertTrue(myPeer1Dir.getDirectory().containsKey(myPeer2.getAddress().toString()));

        Assert.assertTrue(myPeer2Dir.getDirectory().containsKey(myPeer1.getAddress().toString()));
        Assert.assertTrue(myPeer2Dir.getDirectory().containsKey(myPeer2.getAddress().toString()));

        Directory.Entry myPeer1Entry = myPeer1Dir.getDirectory().get(myPeer1.getAddress().toString());

        Assert.assertNotSame(myPeer1Entry.getBorn(), myPeer1Entry.getTimestamp());

        Directory.Entry myPeer2Entry = myPeer2Dir.getDirectory().get(myPeer2.getAddress().toString());

        Assert.assertNotSame(myPeer2Entry.getBorn(), myPeer2Entry.getTimestamp());
    }

    @Test
    public void testAttributes() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8082));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());

        Set<URI> myPeers = new HashSet<URI>();
        myPeers.add(myPeer1.getAddress());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet);

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
    }

    @Test
    public void testListener() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8083));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());
        Peer myPeer2 = new InProcessPeer(myServer, myClient, "/peer2", new Timer());

        Set<URI> myPeers = new HashSet<URI>();
        myPeers.add(myPeer1.getAddress());
        myPeers.add(myPeer2.getAddress());

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        Directory myPeer1Dir = new Directory(myPeer1, myPeerSet);
        Directory myPeer2Dir = new Directory(myPeer2, myPeerSet);
        final AtomicInteger myEventCount = new AtomicInteger(0);

        myPeer1Dir.add(new Directory.Listener() {
            public void updated(Directory aDirectory, List<String> aNewPeers, List<String> anUpdatedPeers) {
                Assert.assertEquals(1, aNewPeers.size());
                Assert.assertEquals(0, anUpdatedPeers.size());

                myEventCount.incrementAndGet();
            }
        });

        myPeer1Dir.start();

        Thread.sleep(1000);

        Assert.assertEquals(2, myPeer1Dir.getDirectory().size());
        Assert.assertEquals(2, myPeer2Dir.getDirectory().size());

        Assert.assertEquals(1, myEventCount.get());
    }
}
