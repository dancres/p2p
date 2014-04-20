package org.dancres.peers.ring;

import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

class RingPositions<T extends Comparable> {
    private final Long _generation;
    private final Set<RingPosition<T>> _positions;

    RingPositions() {
        _generation = 0L;
        _positions = new HashSet<>();
    }

    RingPositions(long aGeneration, HashSet<RingPosition<T>> aPositions) {
        _generation = aGeneration;
        _positions = aPositions;
    }

    boolean supercedes(RingPositions<T> aPositions) {
        return _generation > aPositions._generation;
    }

    RingPositions add(Collection<RingPosition<T>> aPositions) {
        HashSet<RingPosition<T>> myPositions = new HashSet<>(_positions);
        myPositions.addAll(aPositions);

        return new RingPositions(_generation + 1, myPositions);
    }

    RingPositions remove(Collection<RingPosition<T>> aPositions) {
        HashSet<RingPosition<T>> myPositions = new HashSet<>(_positions);
        myPositions.removeAll(aPositions);

        return new RingPositions(_generation + 1, myPositions);
    }

    Set<RingPosition<T>> getPositions() {
        return Collections.unmodifiableSet(_positions);
    }

    public String toString() {
        return "RingPosns: " + _generation + " => " + _positions;
    }

    public boolean equals(Object anObject) {
        if (anObject instanceof RingPositions) {
            RingPositions myPositions = (RingPositions<T>) anObject;

            return ((_generation.equals(myPositions._generation)) &&
                    (Sets.symmetricDifference(_positions, myPositions._positions).size() == 0));
        }

        return false;
    }
}

