package org.dancres.peers.primitives;

import com.ning.http.client.AsyncHttpClient;
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

    public AsyncHttpClient getClient() {
        throw new UnsupportedOperationException();
    }

    public URI getURI() {
        throw new UnsupportedOperationException();
    }

    public void stop() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getAddress() {
        throw new UnsupportedOperationException();
    }

    public void add(Service aService) {
        throw new UnsupportedOperationException();
    }
}
