package org.dancres.peers.primitives;

import org.dancres.peers.PeerSet;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class StaticPeerSet implements PeerSet {
    private final Set<InetSocketAddress> _peerSet;

    public StaticPeerSet(Set<InetSocketAddress> aSet) {
        Set<InetSocketAddress> mySet = new HashSet<InetSocketAddress>();
        mySet.addAll(aSet);

        _peerSet = Collections.unmodifiableSet(mySet);
    }

    public Set<InetSocketAddress> getPeers() {
        Set<InetSocketAddress> myClone = new HashSet<InetSocketAddress>();
        myClone.addAll(_peerSet);

        return myClone;
    }
}
