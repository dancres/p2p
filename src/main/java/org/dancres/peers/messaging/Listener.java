package org.dancres.peers.messaging;

public interface Listener<T> {
    public void arrived(Iterable<T> aMessages);
}

