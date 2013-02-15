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
import java.util.Set;
import java.util.Timer;

public class DirTest {
    @Test
    public void runIt() throws Exception {
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

        myPeer1Dir.start();

        Thread.sleep(1000);

        Assert.assertEquals(2, myPeer1Dir.getDirectory().size());
        Assert.assertEquals(2, myPeer2Dir.getDirectory().size());

        Assert.assertTrue(myPeer1Dir.getDirectory().containsKey(myPeer1.getAddress().toString()));
        Assert.assertTrue(myPeer1Dir.getDirectory().containsKey(myPeer2.getAddress().toString()));
        Assert.assertTrue(myPeer2Dir.getDirectory().containsKey(myPeer1.getAddress().toString()));
        Assert.assertTrue(myPeer2Dir.getDirectory().containsKey(myPeer2.getAddress().toString()));
    }
}
