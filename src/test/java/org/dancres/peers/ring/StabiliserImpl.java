package org.dancres.peers.ring;

public class StabiliserImpl implements ConsistentHash.Listener {

    public void changed(RingSnapshot aSnapshot) {}

    public void rejected(ConsistentHash aRing, RingPosition anOwnedPosition) {
        try {
            aRing.createPosition();
        } catch (Exception anE) {
            throw new RuntimeException("Couldn't allocate a replacement position");
        }
    }
}
