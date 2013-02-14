package org.dancres.peers.primitives;

import org.dancres.peers.Peer;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.internal.ConcurrentWeakKeyHashMap;

import java.net.URI;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ConcurrentMap;

/**
 * InProcessPeer shares a single address and port with a number of other InProcessPeers (e.g. By sharing a single
 * webserver with a common URL base space and mapping to some subspace underneath the base).
 */
public class InProcessPeer implements Peer {
    private final HttpServer _server;
    private final Timer _timer;
    private final ConcurrentMap<String, ServiceDispatcher> _dispatchers =
            new ConcurrentWeakKeyHashMap<String, ServiceDispatcher>();
    private final URI _fullAddress;

    /**
     * @param aServer is the HttpServer to share in
     * @param aPeerAddress is the sub-space to occupy under the HttpServer's base URL - starting with a "/"
     */
    public InProcessPeer(HttpServer aServer, final String aPeerAddress, Timer aTimer) throws Exception {
        _server = aServer;
        _timer = aTimer;
        _fullAddress = new URI(_server.getBase().toString() + aPeerAddress);

        _server.add(aPeerAddress, new HttpServer.Handler() {
            public void process(HttpRequest aRequest, HttpResponse aResponse) {
                String myServicePath =
                        aRequest.getUri().substring(aRequest.getUri().indexOf(aPeerAddress) + aPeerAddress.length());

                for (Map.Entry<String, ServiceDispatcher> kv : _dispatchers.entrySet()) {
                    if (myServicePath.startsWith(kv.getKey())) {
                        kv.getValue().dispatch(myServicePath, aRequest, aResponse);
                        break;
                    }
                }
            }
        });
    }

    public Timer getTimer() {
        return _timer;
    }

    public URI getAddress() {
        return _fullAddress;
    }

    public void add(String aService, ServiceDispatcher aDispatcher) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
