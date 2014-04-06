package org.dancres.peers.messaging;

/**
 * Implement a factory for a subtype of <code>Message</code> so that <code>Channel</code> can generate an
 * stream of messages it can deliver to others in order recovering from message loss.
 *
 * @param <T>
 */
public interface MessageFactory<T> {
    public T newMsg(String aChannelId, long aSeqNum);
}

