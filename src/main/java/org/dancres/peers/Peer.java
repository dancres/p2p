package org.dancres.peers;

import com.ning.http.client.AsyncHttpClient;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.net.URI;
import java.util.Timer;

public interface Peer {
    /**
     * @return the URI at which this peer is rooted
     */
    public URI getAddress();

    /**
     * Add a service to this peer
     *
     * @param aService is the address of the service beginning with a "/" which can be accessed relative to the URI
     *                 from <code>getAddress()</code>
     *
     * @param aDispatcher is the dispatcher that will handle requests for this service.
     */
    public void add(String aService, ServiceDispatcher aDispatcher);

    public Timer getTimer();

    public AsyncHttpClient getClient();

    public interface ServiceDispatcher {
        /**
         * @param aServicePath is the path used to invoke the service (everything after the peer address)
         * @param aRequest is the request information associated with the invocation
         * @param aResponse is the response that will be given to the invocation
         */
        void dispatch(String aServicePath, HttpRequest aRequest, HttpResponse aResponse);
    }
}
