package org.dancres.peers;

import org.dancres.peers.primitives.StaticPeerSet;

public class Directory {
    private final PeerSet _peers;

    public Directory(StaticPeerSet aPeerSet) {
        _peers = aPeerSet;
    }


}
