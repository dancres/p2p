package org.dancres.peers.messaging;

public interface MessageFactory<T> {
    public T newMsg(String aChannelId, long aSeqNum);
}

