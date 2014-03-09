package org.dancres.peers.ring;

import com.google.common.collect.Sets;
import org.dancres.peers.Peer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Ring based on the current positions - the result is a new ring and a record
 * of any rejected node positions
 */
public class RingSnapshot {
    private static final Logger _logger = LoggerFactory.getLogger(RingSnapshot.class);

    final Map<Comparable, RingPosition> _newRing;
    final List<RingPosition> _rejected;
    final Peer _peer;

    RingSnapshot(Map<String, RingPositions> aRingPositions, Peer aPeer) {
        _peer = aPeer;

        Map<Comparable, RingPosition> myNewRing = new HashMap<>();
        List<RingPosition> myLocalRejections = new LinkedList<>();

        for (RingPositions myRingPositions : aRingPositions.values()) {
            for (RingPosition myRingPosn : myRingPositions.getPositions()) {
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
    public RingPosition allocate(Comparable aHashCode) {
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
    public List<RingPosition> allocate(Comparable aHashCode, int aReplicationCount) {
        TreeSet<RingPosition> myPositions = new TreeSet<>(_newRing.values());

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

    private List<RingPosition> extract(TreeSet<RingPosition> aList, RingPosition aFirst, int aNumber) {
        LinkedList<RingPosition> myResults = new LinkedList<>();
        int myTotal = 1;

        myResults.add(aFirst);

        while (myTotal < aNumber) {
            RingPosition myNext = aList.higher(myResults.getLast());

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
    public SortedSet<RingPosition> getPositions() {
        return Collections.unmodifiableSortedSet(
                new TreeSet<>(_newRing.values()));
    }

    class NeighboursSnapshot {
        final HashSet<NeighbourRelation> _neighbours;
        final Set<NeighbourRelation> _changes;

        NeighboursSnapshot(HashSet<NeighbourRelation> aNeighbours, Set<NeighbourRelation> aChanges) {
            _neighbours = aNeighbours;
            _changes = aChanges;
        }
    }

    /**
     * Compute the neighbour position for each of our own positions
     *
     * @param anOldNeighbours
     */
    NeighboursSnapshot computeNeighbours(HashSet<NeighbourRelation> anOldNeighbours) {
        HashSet<NeighbourRelation> myNeighbours = new HashSet<>();
        SortedSet<RingPosition> myRing = new TreeSet<>(_newRing.values());
        RingPosition myLast = myRing.last();

        for (RingPosition myPosn : myRing) {
            if (myPosn.isLocal(_peer) && (! myPosn.equals(myLast))) {
                myNeighbours.add(new NeighbourRelation(myLast, myPosn));
            }

            myLast = myPosn;
        }

        _logger.debug("Neighbour sets: " + anOldNeighbours + " vs\n" + myNeighbours);

        for (NeighbourRelation myNR : anOldNeighbours) {
            _logger.debug("Same: " + myNeighbours.contains(myNR));
        }

        /*
         * JVM Workaround - if this result is not wrapped in a new hashset, the clearAll/addAll in
         * DirectoryListenerImpl.update will cause the changes set to be empty!
         */
        Set<NeighbourRelation> myChanges = Sets.difference(myNeighbours, anOldNeighbours);

        _logger.debug("Neighbour diff: " + myChanges + " " + myChanges.equals(myNeighbours) + " " +
                myChanges.equals(anOldNeighbours));

        // JVM workaround for clear and addAll
        // return new NeighboursRebuild(myNeighbours, new HashSet<NeighbourRelation>(myChanges));

        return new NeighboursSnapshot(myNeighbours, myChanges);
    }

    class NeighbourRelation {
        private final RingPosition _neighbour;
        private final RingPosition _owned;

        NeighbourRelation(RingPosition aNeighbour, RingPosition aLocal) {
            _neighbour = aNeighbour;
            _owned = aLocal;
        }

        public RingPosition getNeighbour() {
            return _neighbour;
        }

        public RingPosition getOwned() {
            return _owned;
        }

        public boolean equals(Object anObject) {
            if (anObject instanceof NeighbourRelation) {
                NeighbourRelation myOther = (NeighbourRelation) anObject;

                return ((_neighbour.equals(myOther._neighbour)) & (_owned.equals(myOther._owned)));
            }

            return false;
        }

        public int hashCode() {
            return _neighbour.hashCode() ^ _owned.hashCode();
        }

        public String toString() {
            return "NRel: " + _neighbour + ", " + _owned;
        }
    }

}

