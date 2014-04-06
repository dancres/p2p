package org.dancres.peers.messaging;

/**
 * Create a Listener implementation and <code>add</code> it to a <code>Channel</code> to receive ordered messages.
 *
 * @param <T>
 */
public interface Listener<T> {
    public void arrived(Iterable<T> aMessages);
}

