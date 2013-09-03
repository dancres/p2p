package org.dancres.peers;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Utility methods for handling <code>PeerSets</code>
 */
public class PeerSets {
    private static final Random _rng = new Random();

    public static URI randomSelect(PeerSet aSet, URI aLocal) {
        Set<URI> myBase = aSet.getPeers();
        myBase.remove(aLocal);

        List<URI> myPeers = new LinkedList<URI>();
        myPeers.addAll(myBase);

        if (myPeers.size() > 0) {
            return myPeers.get(_rng.nextInt(myPeers.size()));
        } else {
            return null;
        }
    }
}
