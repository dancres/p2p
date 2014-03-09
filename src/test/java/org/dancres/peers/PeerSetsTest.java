package org.dancres.peers;

import org.junit.Assert;
import org.dancres.peers.primitives.StaticPeerSet;
import org.junit.Test;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

public class PeerSetsTest {
    @Test
    public void testStatic() throws Exception {
        Set<URI> mySingle = new HashSet<>();
        mySingle.add(new URI("http://localhost:8080"));

        Set<URI> myDouble = new HashSet<>();
        myDouble.add(new URI("http://localhost:8080"));
        myDouble.add(new URI("http://localhost:8081"));

        StaticPeerSet mySet = new StaticPeerSet(mySingle);

        Assert.assertEquals(1, mySet.getPeers().size());
        Assert.assertNull(PeerSets.randomSelect(mySet, new URI("http://localhost:8080")));

        mySet = new StaticPeerSet(myDouble);

        Assert.assertEquals(2, mySet.getPeers().size());

        URI myChosen =
                PeerSets.randomSelect(mySet, new URI("http://localhost:8080"));

        Assert.assertNotNull(myChosen);
        Assert.assertNotSame(new URI("http://localhost:8080"), myChosen);
    }
}
