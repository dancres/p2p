package org.dancres.peers.ring;

import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

class RingPositions {
    private final Long _generation;
    private final Set<RingPosition> _positions;

    RingPositions() {
        _generation = 0L;
        _positions = new HashSet<RingPosition>();
    }

    RingPositions(long aGeneration, HashSet<RingPosition> aPositions) {
        _generation = aGeneration;
        _positions = aPositions;
    }

    boolean supercedes(RingPositions aPositions) {
        return _generation > aPositions._generation;
    }

    RingPositions add(Collection<RingPosition> aPositions) {
        HashSet<RingPosition> myPositions = new HashSet<RingPosition>(_positions);
        myPositions.addAll(aPositions);

        return new RingPositions(_generation + 1, myPositions);
    }

    RingPositions remove(Collection<RingPosition> aPositions) {
        HashSet<RingPosition> myPositions = new HashSet<RingPosition>(_positions);
        myPositions.removeAll(aPositions);

        return new RingPositions(_generation + 1, myPositions);
    }

    Set<RingPosition> getPositions() {
        return Collections.unmodifiableSet(_positions);
    }

    public String toString() {
        return "RingPosns: " + _generation + " => " + _positions;
    }

    public boolean equals(Object anObject) {
        if (anObject instanceof RingPositions) {
            RingPositions myPositions = (RingPositions) anObject;

            return ((_generation.equals(myPositions._generation)) &&
                    (Sets.symmetricDifference(_positions, myPositions._positions).size() == 0));
        }

        return false;
    }
}

