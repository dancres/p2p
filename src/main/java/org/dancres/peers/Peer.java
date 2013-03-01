package org.dancres.peers;

import com.ning.http.client.AsyncHttpClient;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.net.URI;
import java.util.Timer;

public interface Peer {
    /**
     *
     */
    public URI getURI();

    public void stop();

    /**
     * @return the URI at which this peer is rooted
     */
    public String getAddress();

    public Service find(Class aServiceClass);

    /**
     * Add a service to this peer
     */
    public void add(Service aService);

    public Timer getTimer();

    public AsyncHttpClient getClient();

    public interface Service {
        String getAddress();
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
