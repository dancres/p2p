package org.dancres.peers;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.util.TimerTask;

public class Directory {
    private final PeerSet _peers;
    private final Peer _peer;
    private final Peer.ServiceDispatcher _dispatcher;

    public Directory(Peer aPeer, PeerSet aPeerSet) {
        _peers = aPeerSet;
        _peer = aPeer;
        _dispatcher = new Dispatcher();
    }

    public void start() {
        _peer.add("directory", _dispatcher);
        _peer.getTimer().schedule(new GossipTask(), 0, 30000);
    }

    class Dispatcher implements Peer.ServiceDispatcher {
        public void dispatch(String aServicePath, HttpRequest aRequest, HttpResponse aResponse) {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    class GossipTask extends TimerTask {

        public void run() {
        }
    }
}
