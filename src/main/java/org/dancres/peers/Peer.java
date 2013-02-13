package org.dancres.peers;

public interface Peer {
    /**
     * @return the URI at which this peer is rooted
     */
    public String getAddress();

    /**
     * Add a service to this peer
     *
     * @param aService is the address of the service which can be accessed relative to the URI from
     *                 <code>getAddress()</code>
     * @param aDispatcher is the dispatcher that will handle requests for this service.
     */
    public void add(String aService, ServiceDispatcher aDispatcher);

    public interface ServiceDispatcher {
    }
}
