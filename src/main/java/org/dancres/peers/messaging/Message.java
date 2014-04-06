package org.dancres.peers.messaging;

/**
 * The contract for a message that <code>Channel</code> can deliver with ordering and recovery guarantees.
 */
public interface Message {
    public String getChannelId();
    public long getSeq();
}
