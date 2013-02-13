package org.dancres.peers;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class PeerSets {
    private static final Random _rng = new Random();

    public static InetSocketAddress randomSelect(PeerSet aSet, InetSocketAddress aLocal) {
        Set<InetSocketAddress> myBase = aSet.getPeers();
        myBase.remove(aLocal);

        List<InetSocketAddress> myPeers = new LinkedList<InetSocketAddress>();
        myPeers.addAll(myBase);

        if (myPeers.size() > 0) {
            return myPeers.get(_rng.nextInt(myPeers.size()));
        } else {
            return null;
        }
    }
}
