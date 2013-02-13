package org.dancres.peers;

import junit.framework.Assert;
import org.dancres.peers.primitives.StaticPeerSet;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

public class PeerSetsTest {
    @Test
    public void testStatic() {
        Set<InetSocketAddress> mySingle = new HashSet<InetSocketAddress>();
        mySingle.add(InetSocketAddress.createUnresolved("localhost", 8080));

        Set<InetSocketAddress> myDouble = new HashSet<InetSocketAddress>();
        myDouble.add(InetSocketAddress.createUnresolved("localhost", 8080));
        myDouble.add(InetSocketAddress.createUnresolved("localhost", 8081));

        StaticPeerSet mySet = new StaticPeerSet(mySingle);

        Assert.assertEquals(1, mySet.getPeers().size());
        Assert.assertNull(PeerSets.randomSelect(mySet, InetSocketAddress.createUnresolved("localhost", 8080)));

        mySet = new StaticPeerSet(myDouble);

        Assert.assertEquals(2, mySet.getPeers().size());

        InetSocketAddress myChosen =
                PeerSets.randomSelect(mySet, InetSocketAddress.createUnresolved("localhost", 8080));

        Assert.assertNotNull(myChosen);
        Assert.assertNotSame(InetSocketAddress.createUnresolved("localhost", 8080), myChosen);
    }
}
