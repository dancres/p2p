package org.dancres.peers;

import java.net.URI;
import java.util.Set;

/**
 * A group of peers as might be configured to work as a cluster or perhaps to be the core set around which all
 * other peers are arranged.
 */
public interface PeerSet {
    public Set<URI> getPeers();
}
