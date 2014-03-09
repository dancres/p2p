package org.dancres.peers.primitives;

import org.dancres.peers.PeerSet;

import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A static (unchanging set of peers) implementation of a <code>PeerSet</code>
 */
public class StaticPeerSet implements PeerSet {
    private final Set<URI> _peerSet;

    public StaticPeerSet(Set<URI> aSet) {
        Set<URI> mySet = new HashSet<>();
        mySet.addAll(aSet);

        _peerSet = Collections.unmodifiableSet(mySet);
    }

    public Set<URI> getPeers() {
        Set<URI> myClone = new HashSet<>();
        myClone.addAll(_peerSet);

        return myClone;
    }

    public Set<String> getPeersAsStrings() {
        Set<String> myClone = new HashSet<>();

        for (URI aP : _peerSet)
            myClone.add(aP.toString());

        return myClone;
    }
}
