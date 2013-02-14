package org.dancres.peers.primitives;

import org.dancres.peers.PeerSet;

import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class StaticPeerSet implements PeerSet {
    private final Set<URI> _peerSet;

    public StaticPeerSet(Set<URI> aSet) {
        Set<URI> mySet = new HashSet<URI>();
        mySet.addAll(aSet);

        _peerSet = Collections.unmodifiableSet(mySet);
    }

    public Set<URI> getPeers() {
        Set<URI> myClone = new HashSet<URI>();
        myClone.addAll(_peerSet);

        return myClone;
    }
}
