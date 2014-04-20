package org.dancres.peers.ring;

import org.dancres.peers.Peer;

public class RingPosition<T extends Comparable> implements Comparable<RingPosition<T>> {
    private final String _peerName;
    private final T _position;
    private final long _birthDate;

    RingPosition(Peer aPeer, T aPosition) {
        this(aPeer, aPosition, System.currentTimeMillis());
    }

    RingPosition(Peer aPeer, T aPosition, long aBirthDate) {
        this(aPeer.getAddress(), aPosition, aBirthDate);
    }

    RingPosition(String aPeerAddr, T aPosition, long aBirthDate) {
        if (aPosition == null)
            throw new IllegalArgumentException();

        _peerName = aPeerAddr;
        _position = aPosition;
        _birthDate = aBirthDate;
    }

    public T getPosition() {
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

    public int compareTo(RingPosition<T> anOther) {

        return (_position.compareTo(anOther._position));
    }

    public int hashCode() {
        int myFirst = _peerName.hashCode();
        int mySecond = _position.hashCode();

        return myFirst ^ mySecond;
    }

    public boolean equals(Object anObject) {
        if (anObject instanceof RingPosition) {
            RingPosition myOther = (RingPosition<T>) anObject;

            if (_peerName.equals(myOther._peerName))
                return (compareTo(myOther) == 0);
        }

        return false;
    }

    public String toString() {
        return "RingPosn: " + _position + " @ " + _peerName + " born: " + _birthDate;
    }
}

