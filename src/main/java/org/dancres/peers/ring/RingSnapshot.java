package org.dancres.peers.ring;

import com.google.common.collect.Iterables;
import org.dancres.peers.Peer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Ring based on the current positions - the result is a new ring and a record
 * of any rejected node positions
 */
public class RingSnapshot<T extends Comparable> implements Iterable<RingPosition<T>> {
    private static final Logger _logger = LoggerFactory.getLogger(RingSnapshot.class);

    final Map<T, RingPosition<T>> _newRing;
    final List<RingPosition<T>> _rejected;
    final Peer _peer;

    RingSnapshot(Map<String, RingPositions<T>> aRingPositions, Peer aPeer) {
        _peer = aPeer;

        Map<T, RingPosition<T>> myNewRing = new HashMap<>();
        List<RingPosition<T>> myLocalRejections = new LinkedList<>();

        for (RingPositions<T> myRingPositions : aRingPositions.values()) {
            for (RingPosition<T> myRingPosn : myRingPositions.getPositions()) {
                RingPosition myConflict = myNewRing.get(myRingPosn.getPosition());

                if (myConflict == null) {
                    myNewRing.put(myRingPosn.getPosition(), myRingPosn);
                } else {
                    RingPosition myLoser;

                    _logger.debug("Got position conflict: " + myConflict + ", " + myRingPosn);

                    if (myConflict.bounces(myRingPosn)) {
                        _logger.debug("Loser in conflict (new posn): " + myRingPosn);

                        myLoser = myRingPosn;

                    } else {
                        _logger.debug("Loser in conflict (conflict): " + myConflict);

                        myLoser = myConflict;
                        myNewRing.put(myRingPosn.getPosition(), myRingPosn);
                    }

                    // Are we the losing peer?
                    //
                    if (myLoser.isLocal(_peer)) {
                        _logger.debug("We are the losing peer");

                        myLocalRejections.add(myLoser);
                    }
                }
            }
        }

        _newRing = myNewRing;
        _rejected = myLocalRejections;
    }

    /**
     * Takes a hashcode and returns the position to allocate it to.
     *
     * @param aHashCode
     * @return
     */
    public RingPosition<T> allocate(Comparable aHashCode) {
        return allocate(aHashCode, 1).get(0);
    }

    /**
     * Takes a hashcode and returns the position(s) to allocate it to.
     *
     * @param aHashCode
     * @param aReplicationCount the number of positions to return
     *
     * @return a list of positions
     */
    public List<RingPosition<T>> allocate(Comparable aHashCode, int aReplicationCount) {
        TreeSet<RingPosition<T>> myPositions = new TreeSet<>(_newRing.values());

        if (myPositions.size() == 0)
            throw new IllegalStateException("Haven't got any positions to allocate to");

        if (myPositions.size() < aReplicationCount)
            throw new IllegalStateException("Haven't got enough positions for the specified replication count: " +
                    aReplicationCount);

        // If aHashCode is greater than the greatest position, it wraps around to the first
        //
        if (myPositions.last().getPosition().compareTo(aHashCode) < 1)
            return extract(myPositions, myPositions.first(), aReplicationCount);
        else {
            for (RingPosition myPos : myPositions) {
                if (myPos.getPosition().compareTo(aHashCode) >= 1) {
                    return extract(myPositions, myPos, aReplicationCount);
                }
            }
        }

        // Shouldn't happen
        //
        throw new RuntimeException("Logical error in code");
    }

    private List<RingPosition<T>> extract(TreeSet<RingPosition<T>> aList, RingPosition<T> aFirst, int aNumber) {
        LinkedList<RingPosition<T>> myResults = new LinkedList<>();
        int myTotal = 1;

        myResults.add(aFirst);

        while (myTotal < aNumber) {
            RingPosition<T> myNext = aList.higher(myResults.getLast());

            if (myNext == null) {
                // Wrap
                //
                myResults.add(aList.first());
            } else {
                myResults.add(myNext);
            }

            myTotal++;
        }

        return myResults;
    }

    /**
     * @return this peer's current view of the ring
     */
    public SortedSet<RingPosition<T>> getPositions() {
        return Collections.unmodifiableSortedSet(
                new TreeSet<>(_newRing.values()));
    }

    /**
     * @return An immutable infinite iteration of all the ring positions in this snapshot
     */
    public Iterator<RingPosition<T>> iterator() {
        return Iterables.cycle(Collections.unmodifiableList(new LinkedList<>(getPositions()))).iterator();
    }

    public Iterator<RingPosition<T>> reverseIterator() {
        LinkedList<RingPosition<T>> myReverse = new LinkedList<>(getPositions());
        Collections.reverse(myReverse);

        return Iterables.cycle(Collections.unmodifiableList(myReverse)).iterator();
    }
}

