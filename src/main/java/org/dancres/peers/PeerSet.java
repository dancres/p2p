package org.dancres.peers;

import java.net.InetSocketAddress;
import java.util.Set;

public interface PeerSet {
    public Set<InetSocketAddress> getPeers();
}
