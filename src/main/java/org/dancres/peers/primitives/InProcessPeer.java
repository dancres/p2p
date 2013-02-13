package org.dancres.peers.primitives;

import org.dancres.peers.Peer;

/**
 * InProcessPeer shares a single address and port with a number of other InProcessPeers (e.g. By sharing a single
 * webserver with a common URL base space and mapping to some subspace underneath the base).
 */
public class InProcessPeer implements Peer {
    /**
     * @param aServer is the HttpServer to share in
     * @param aPeerAddress is the sub-space to occupy under the HttpServer's base URL
     */
    public InProcessPeer(HttpServer aServer, String aPeerAddress) {
    }

    public String getAddress() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void add(String aService, ServiceDispatcher aDispatcher) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
