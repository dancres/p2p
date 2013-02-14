package org.dancres.peers.primitives;

import org.dancres.peers.Peer;

import java.net.URI;
import java.util.Timer;

/**
 * SingleProcessPeer is a single-process peer (e.g. it's a standalone webserver that runs on a particular address).
 */
public class SingleProcessPeer implements Peer {
    public Timer getTimer() {
        return null;
    }

    public URI getAddress() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void add(String aService, ServiceDispatcher aDispatcher) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
