package org.dancres.peers.acc;

import com.ning.http.client.AsyncHttpClient;
import org.dancres.peers.Peer;
import org.dancres.net.netty.HttpServer;
import org.dancres.peers.primitives.InProcessPeer;
import org.junit.Test;
import org.junit.Assert;

import java.net.InetSocketAddress;
import java.util.Timer;

public class LoopbackTest {
    @Test
    public void loopback() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8081));
        AsyncHttpClient myClient = new AsyncHttpClient();
        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());

        DecayingAccumulators myCounts = new DecayingAccumulators(myPeer1, 2000);

        DecayingAccumulators.Count myTotal = myCounts.log(myPeer1.getAddress(),
                myCounts.newCount("a", 1000, 1000));

        Assert.assertEquals(1000, myTotal.getCount());

        DecayingAccumulators.Count myCurrent = myCounts.get(myPeer1.getAddress(), "a");

        Assert.assertEquals(1000, myCurrent.getCount());

        myTotal = myCounts.log(myPeer1.getAddress(), myCounts.newCount("a", 1000, 1000));

        Assert.assertEquals(2000, myTotal.getCount());

        Thread.sleep(3000);

        myTotal = myCounts.log(myPeer1.getAddress(), myCounts.newCount("a", 1000, 1000));

        Assert.assertEquals(1000, myTotal.getCount());

        myPeer1.stop();
        myServer.terminate();
    }
}
