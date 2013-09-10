package org.dancres.peers.ring;

import org.dancres.peers.Peer;

public class RingPosition implements Comparable {
    private final String _peerName;
    private final Comparable _position;
    private final long _birthDate;

    RingPosition(Peer aPeer, Comparable aPosition) {
        this(aPeer, aPosition, System.currentTimeMillis());
    }

    RingPosition(Peer aPeer, Comparable aPosition, long aBirthDate) {
        this(aPeer.getAddress(), aPosition, aBirthDate);
    }

    RingPosition(String aPeerAddr, Comparable aPosition, long aBirthDate) {
        if (aPosition == null)
            throw new IllegalArgumentException();

        _peerName = aPeerAddr;
        _position = aPosition;
        _birthDate = aBirthDate;
    }

    public Comparable getPosition() {
        return _position;
    }

    public String getPeerAddress() {
        return _peerName;
    }

    long getBirthDate() {
        return _birthDate;
    }

    boolean bounces(RingPosition anotherPosn) {
        return _birthDate < anotherPosn._birthDate;
    }

    boolean isLocal(Peer aPeer) {
        return _peerName.equals(aPeer.getAddress());
    }

    public int compareTo(Object anObject) {
        RingPosition myOther = (RingPosition) anObject;

        return (_position.compareTo(myOther._position));
    }

    public int hashCode() {
        int myFirst = _peerName.hashCode();
        int mySecond = _position.hashCode();

        return myFirst ^ mySecond;
    }

    public boolean equals(Object anObject) {
        if (anObject instanceof RingPosition) {
            RingPosition myOther = (RingPosition) anObject;

            if (_peerName.equals(myOther._peerName))
                return (compareTo(anObject) == 0);
        }

        return false;
    }

    public String toString() {
        return "RingPosn: " + _position + " @ " + _peerName + " born: " + _birthDate;
    }
}

