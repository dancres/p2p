package org.dancres.peers.ring;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Neighbours {
    public static <T extends Comparable> List<RingPosition<T>> getLowerNeighbours(ConsistentHash<T> aHash,
                                                                            RingPosition<T> aPosn, int aMaxNeigbours) {
        return getNeighbours(aHash.getRing().reverseIterator(), aPosn, aMaxNeigbours);
    }

    public static <T extends Comparable> List<RingPosition<T>> getUpperNeighbours(ConsistentHash<T> aHash,
                                                                            RingPosition<T> aPosn, int aMaxNeigbours) {
        return getNeighbours(aHash.getRing().iterator(), aPosn, aMaxNeigbours);
    }

    public static <T extends Comparable> List<RingPosition<T>> getNeighbours(Iterator<RingPosition<T>> aRingPosns,
                                                                             RingPosition<T> aPosn, int aMaxNeighbours) {
        while (! aRingPosns.next().getPosition().equals(aPosn.getPosition()));

        List<RingPosition<T>> myNeigbours = new LinkedList<>();
        for (int i = 0; i < aMaxNeighbours; i++)
            myNeigbours.add(aRingPosns.next());

        return myNeigbours;
    }
}
