package org.dancres.peers.primitives;

import org.dancres.peers.Peer;

/**
 * SingleProcessPeer is a single-process peer (e.g. it's a standalone webserver that runs on a particular address).
 */
public class SingleProcessPeer implements Peer {
    public String getAddress() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void add(String aService, ServiceDispatcher aDispatcher) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}