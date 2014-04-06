package org.dancres.peers.messaging;

public interface Message {
    public String getChannelId();
    public long getSeq();
}
