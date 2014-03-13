package org.dancres.peers;

import com.ning.http.client.AsyncHttpClient;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.net.URI;
import java.util.Timer;

/**
 * The core abstraction for the toolkit. A <code>Peer</code> represents an independent entity running a set of services,
 * co-operating with some set of peers. Interactions across peers are run across http connections with services mapped
 * to specific namespaces under "{@literal <}peer_address:peer_port{@literal >}".
 */
public interface Peer {
    /**
     * Shutdown this peer
     */
    public void stop();

    /**
     * @return the URI at which this peer is rooted
     */
    public URI getURI();

    /**
     * @return astring representation of the URI at which this peer is rooted
     */
    public String getAddress();

    /**
     * @param aServiceClass is the class of the service to find
     * @return the registered instance of the specified service or <code>null</code> if it's not present.
     */
    public Service find(Class aServiceClass);

    /**
     * Add a service to this peer
     */
    public void add(Service aService);

    /**
     * Peer's provide a timer instance which can be shared across registered services for purposes of running
     * regular or scheduled tasks.
     *
     * @return the peer's timer instance
     */
    public Timer getTimer();

    /**
     * @return a reference to the underlying comms stack being used by the peer.
     */
    public AsyncHttpClient getClient();

    public interface Service {
        /**
         * @return the path for this service as registered under the peer
         */
        String getAddress();

        /**
         * @return the dispatcher this service wishes to have used by the peer for handling requests.
         */
        ServiceDispatcher getDispatcher();
    }

    public interface ServiceDispatcher {
        /**
         * @param aServicePath is the path used to invoke the service (everything after the peer address)
         * @param aRequest is the request information associated with the invocation
         * @param aResponse is the response that will be given to the invocation
         */
        void dispatch(String aServicePath, HttpRequest aRequest, HttpResponse aResponse);
    }
}
