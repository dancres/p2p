package org.dancres.peers.ring;

public class StabiliserImpl implements ConsistentHash.Listener {
    public void newNeighbour(ConsistentHash aRing, RingPosition anOwnedPosition,
                             RingPosition aNeighbourPosition) {
        // Doesn't matter
    }

    public void rejected(ConsistentHash aRing, RingPosition anOwnedPosition) {
        try {
            aRing.createPosition();
        } catch (Exception anE) {
            throw new RuntimeException("Couldn't allocate a replacement position");
        }
    }
}
