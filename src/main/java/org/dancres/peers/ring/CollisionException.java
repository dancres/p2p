package org.dancres.peers.ring;

public class CollisionException extends Exception {
    CollisionException(String aReason) {
        super(aReason);
    }
}
