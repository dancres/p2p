package org.dancres.peers;

import java.util.*;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ring position leasing, ring position birth dates etc
 *
 * Allocate ourselves some set of ring positions, each with an id (say a random integer which fits nicely with
 * hashCode()).
 *
 * Each ring position has a birth date which we can use to resolve collisions.
 *
 * As each node in the ring can perform the hash and determine the ring position to assign it a uniquely identified
 * (UUID or String or ?) value to, any client can write to any node although it would be best if the client cached
 * the map itself (implement this later). We will store the key, value and the key hashcode.
 *
 * The Directory will announce the comings and goings of nodes and we will interrogate new or updated nodes for their
 * ring positions, dumping any they were previously associated with.
 *
 * Should we detect a ring position that clashes with our locally allocated ring positions we first compare birth dates
 * and the oldest one wins. If that doesn't work, we consider peer ids and chose a winner from that (lexically or
 * maybe hashCode comparison).
 *
 * Once a winner is determined, the loser is re-numbered randomly and then a job is kicked off to migrate any values to
 * the winner ring position (those with a key that hashes to an appropriate value).
 *
 * A similar value migration strategy is adopted when we detect a new ring position that is adjacent (that is it is less
 * than ours but greater than all others less than ours) to ours. We kick a job off to migrate any values to this
 * new ring position.
 *
 * ring positions can be maintained by a different service within the peer and it will support a migrate function that
 * moves values between ring positions.
 *
 * "Each data item identified by a key is assigned to a node by hashing the data item’s key to yield its position on
 * the ring, and then walking the ring clockwise to find the first node with a position larger than the item’s
 * position."
 *
 * Might want to leave migration to users of this code. Leaving us to figure out when there are new neighbours
 * and collisions?
 *
 * @todo Multi-threading
 */
public class ConsistentHashRing {
    private static final String RING_MEMBERSHIP = "org.dancres.peers.consistentHashRing.ringMembership";

    private static final Logger _logger = LoggerFactory.getLogger(ConsistentHashRing.class);

    public static class RingPosition implements Comparable {
        private String _peerName;
        private Integer _position;
        private long _birthDate;

        RingPosition(Peer aPeer, Integer aPosition) {
            this(aPeer, aPosition, System.currentTimeMillis());
        }

        RingPosition(Peer aPeer, Integer aPosition, long aBirthDate) {
            _peerName = aPeer.getAddress();
            _position = aPosition;
            _birthDate = aBirthDate;
        }

        public Integer getPosition() {
            return _position;
        }

        boolean bounces(RingPosition anotherPosn) {
            return _birthDate < anotherPosn._birthDate;
        }

        boolean isLocal(Peer aPeer) {
            return _peerName.equals(aPeer.getAddress());
        }

        public int compareTo(Object anObject) {
            RingPosition myOther = (RingPosition) anObject;

            return (_position - myOther._position);
        }

        public int hashCode() {
            return _peerName.hashCode() ^ _position.hashCode();
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

    public static class RingPositions {
        private Long _generation;
        private final List<RingPosition> _positions;

        RingPositions() {
            _generation = 0L;
            _positions = new LinkedList<RingPosition>();
        }

        boolean supercedes(RingPositions aPositions) {
            return _generation > aPositions._generation;
        }

        void add(RingPosition aPos) {
            _generation++;
            _positions.add(aPos);
        }

        void remove(RingPosition aPos) {
            _generation++;
            _positions.remove(aPos);
        }

        public List<RingPosition> getPositions() {
            return Collections.unmodifiableList(_positions);
        }

        public String toString() {
            return "RingPosns: " + _generation + " => " + _positions;
        }
    }

    public static class NeighbourRelation {
        private RingPosition _neighbour;
        private RingPosition _owned;

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

    private final Peer _peer;
    private final Directory _dir;
    private final Random _rng = new Random();

    /**
     * The current view of the hash ring
     */
    private final Map<Integer, RingPosition> _allPositions;

    /**
     * The positions held by each node identified by address
     */
    private final Map<String, RingPositions> _ringPositions;

    /**
     * The neighbour relations
     */
    private HashSet<NeighbourRelation> _neighbours = new HashSet<NeighbourRelation>();

    private final List<Listener> _listeners = new LinkedList<Listener>();

    public ConsistentHashRing(Peer aPeer, Directory aDirectory) {
        _peer = aPeer;
        _dir = aDirectory;
        _allPositions = new HashMap<Integer, RingPosition>();
        _ringPositions = new HashMap<String, RingPositions>();
        _ringPositions.put(_peer.getAddress(), new RingPositions());

        _dir.add(new AttrProducerImpl());
        _dir.add(new DirListenerImpl());
    }

    private class AttrProducerImpl implements Directory.AttributeProducer {
        public Map<String, String> produce() {
            Map<String, String> myFlattenedRingPosns = new HashMap<String, String>();

            myFlattenedRingPosns.put(RING_MEMBERSHIP, flattenRingPositions(_ringPositions.get(_peer.getAddress())));

            return myFlattenedRingPosns;
        }
    }

    private String flattenRingPositions(RingPositions aPositions) {
        return new Gson().toJson(_ringPositions.get(_peer.getAddress()));
    }

    private RingPositions extractRingPositions(Directory.Entry anEntry) {
        return new Gson().fromJson(anEntry.getAttributes().get(RING_MEMBERSHIP), RingPositions.class);
    }

    private class DirListenerImpl implements Directory.Listener {
        /**
         * A locally inserted position will be communicated to other nodes as we gossip. Other nodes though may
         * introduce no changes to the ring. <code>updated</code> is called for each gossip and sweep through the
         * directory whether there are changes or not. We rely on this to run a conflict resolution sweep
         * to do conflict resolution on our locally inserted positions. Thus at this moment we cannot avoid doing
         * conflict resolution in absence of ring changes.
         *
         * @todo Modify new/insertPosition to do a sweep for conflict resolution so that we can implement a no
         * gossip'd updates, no sweep optimisation.
         *
         * @param aDirectory
         * @param aNewPeers
         * @param anUpdatedPeers
         */
        public void updated(Directory aDirectory, List<Directory.Entry> aNewPeers,
                            List<Directory.Entry> anUpdatedPeers) {

            _logger.debug("Ring Update");

            for (Directory.Entry aNewEntry : Iterables.filter(aNewPeers, new Predicate<Directory.Entry>() {
                public boolean apply(Directory.Entry entry) {
                    return entry.getAttributes().containsKey(RING_MEMBERSHIP);
                }
            })) {
                RingPositions myPeerPositions = extractRingPositions(aNewEntry);

                _logger.debug("New positions from new: " + aNewEntry.getPeerName(), myPeerPositions);

                _ringPositions.put(aNewEntry.getPeerName(), myPeerPositions);
            }

            for (Directory.Entry anUpdatedEntry : Iterables.filter(anUpdatedPeers, new Predicate<Directory.Entry>() {
                public boolean apply(Directory.Entry entry) {
                    return entry.getAttributes().containsKey(RING_MEMBERSHIP);
                }
            })) {
                RingPositions myPeerPositions = extractRingPositions(anUpdatedEntry);
                RingPositions myPrevious = _ringPositions.get(anUpdatedEntry.getPeerName());

                // Was the positions list updated?
                //
                if ((myPrevious == null) ||
                        (myPeerPositions.supercedes(_ringPositions.get(anUpdatedEntry.getPeerName())))) {

                    if (myPrevious == null)
                        _logger.debug("New positions from: " + anUpdatedEntry.getPeerName(), myPeerPositions);
                    else
                        _logger.debug("Updated positions from: " + anUpdatedEntry.getPeerName(), myPeerPositions);

                    _ringPositions.put(anUpdatedEntry.getPeerName(), myPeerPositions);
                }
            }

            /*
             * Re-build the ring from _ringPositions
             *
             * Doing collision resolution as we go. In the case where one of our positions is the loser, remove it
             * and report it to listeners.
             */
            _allPositions.clear();

            List<RingPosition> myLocalRejections = new LinkedList<RingPosition>();

            for (Map.Entry<String, RingPositions> myPeerAndPositions : _ringPositions.entrySet()) {
                for (RingPosition myRingPosn : myPeerAndPositions.getValue().getPositions()) {
                    RingPosition myConflict = _allPositions.get(myRingPosn.getPosition());

                    if (myConflict == null) {
                        _allPositions.put(myRingPosn.getPosition(), myRingPosn);
                    } else {
                        _logger.debug("Got position conflict: " + myConflict + ", " + myRingPosn);

                        if (myConflict.bounces(myRingPosn)) {
                            _logger.debug("Loser in conflict (new posn): " + myRingPosn);

                            // Are we the losing peer?
                            //
                            if (myRingPosn.isLocal(_peer)) {
                                _logger.debug("We are the losing peer");

                                for (Listener anL : _listeners) {
                                    anL.rejected(myRingPosn);
                                }

                                myLocalRejections.add(myRingPosn);
                            }
                        } else {
                            _logger.debug("Loser in conflict (conflict): " + myConflict);

                            _allPositions.put(myRingPosn.getPosition(), myRingPosn);
                        }
                    }
                }
            }

            if (! myLocalRejections.isEmpty()) {
                RingPositions myPosns = _ringPositions.get(_peer.getAddress());

                for (RingPosition myPosn : myLocalRejections)
                    myPosns.remove(myPosn);
            }

            // No point in a diff if we're empty
            //
            if (_allPositions.isEmpty())
                return;

            // Note that if the old neighbour's position was higher than the new, there is no need to report a change
            // because nothing would need moving but perhaps we leave that smart to the upper layers?
            //
            HashSet<NeighbourRelation> myNeighbours = new HashSet<NeighbourRelation>();
            SortedSet<RingPosition> myRing = new TreeSet<RingPosition>(_allPositions.values());
            RingPosition myLast = myRing.last();

            for (RingPosition myPosn : myRing) {
                if (myPosn.isLocal(_peer) && (! myPosn.equals(myLast))) {
                    myNeighbours.add(new NeighbourRelation(myLast, myPosn));
                }

                myLast = myPosn;
            }

            _logger.debug("Neighbour sets: " + _neighbours + " vs\n" + myNeighbours);

            for (NeighbourRelation myNR : _neighbours) {
                _logger.debug("Same: " + myNeighbours.contains(myNR));
            }

            Set<NeighbourRelation> myChanges = Sets.difference(myNeighbours, _neighbours);

            _logger.debug("Neighbour diff: " + myChanges);

            if (! myChanges.isEmpty()) {
                _neighbours = myNeighbours;

                for (Listener myL : _listeners)
                    for (NeighbourRelation myChange : myChanges)
                        myL.newNeighbour(myChange._owned, myChange._neighbour);
            }
        }
    }

    RingPosition insertPosition(RingPosition aPosn) {
        _ringPositions.get(_peer.getAddress()).add(aPosn);
        _allPositions.put(aPosn.getPosition(), aPosn);

        return aPosn;
    }

    public Set<NeighbourRelation> getNeighbours() {
        return Collections.unmodifiableSet(_neighbours);
    }

    public Collection<RingPosition> getCurrentRing() {
        return Collections.unmodifiableCollection(_allPositions.values());
    }

    public RingPositions getCurrentPositions() {
        return _ringPositions.get(_peer.getAddress());
    }

    public RingPosition newPosition() {
        RingPosition myNewPos;

        do {
            myNewPos = new RingPosition(_peer, _rng.nextInt());
        } while (_allPositions.get(myNewPos.getPosition()) != null);

        return insertPosition(myNewPos);
    }

    public void add(Listener aListener) {
        _listeners.add(aListener);
    }

    /**
     * Takes a hashCode and returns the container to allocate it to.
     *
     * @param aHashCode
     * @return
     */
    public RingPosition allocate(Integer aHashCode) {
        throw new UnsupportedOperationException();
    }

    public static interface Listener {
        public void newNeighbour(RingPosition anOwnedPosition, RingPosition aNeighbourPosition);

        public void rejected(RingPosition anOwnedPosition);
    }
}
