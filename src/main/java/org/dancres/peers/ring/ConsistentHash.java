package org.dancres.peers.ring;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.gson.*;
import org.dancres.peers.Directory;
import org.dancres.peers.Peer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsistentHash {
    private static final String RING_MEMBERSHIP = "org.dancres.peers.ring.consistentHash.ringMembership";

    private static final Logger _logger = LoggerFactory.getLogger(ConsistentHash.class);

    public static interface PositionGenerator {
        public Comparable newId();
    }

    public static interface PositionPacker {
        public Comparable unpack(String aPacked);
        public String pack(Comparable anId);
    }

    public static interface Listener {
        public void newNeighbour(ConsistentHash aRing, RingPosition anOwnedPosition, RingPosition aNeighbourPosition);
        public void rejected(ConsistentHash aRing, RingPosition anOwnedPosition);
    }

    public static class NeighbourRelation {
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

    private final Peer _peer;
    private final Directory _dir;
    private final List<Listener> _listeners = new CopyOnWriteArrayList<Listener>();

    /**
     * The positions held by each node identified by address
     */
    private final ConcurrentMap<String, RingPositions> _ringPositions = new ConcurrentHashMap<String, RingPositions>();

    /**
     * The neighbour relations - which positions are closest whilst still less than our own
     */
    private final AtomicReference<HashSet<NeighbourRelation>> _neighbours =
            new AtomicReference<HashSet<NeighbourRelation>>(new HashSet<NeighbourRelation>());

    private final Packager _packager;
    private final PositionGenerator _positionGenerator;

    public ConsistentHash(Peer aPeer, PositionGenerator aGenerator, PositionPacker aPacker) {
        if (aGenerator == null)
            throw new IllegalArgumentException("Generator cannot be null");

        if (aPacker == null)
            throw new IllegalArgumentException("Packer cannot be null");

        _peer = aPeer;
        _positionGenerator = aGenerator;
        _packager = new Packager(aPacker, RING_MEMBERSHIP);
        _dir = (Directory) aPeer.find(Directory.class);

        if (_dir == null)
            throw new RuntimeException("ConsistentHash couldn't locate a Directory service in peer");

        _ringPositions.put(_peer.getAddress(), new RingPositions());

        _dir.add(new AttrProducerImpl());
        _dir.add(new DirListenerImpl());
    }

    public ConsistentHash(Peer aPeer) {
        this(aPeer,
                new PositionGenerator() {
                    private Random _rng = new Random();

                    public Comparable newId() {
                        return _rng.nextInt();
                    }
                },

                new PositionPacker() {
                    public Comparable unpack(String aPacked) {
                        return Integer.parseInt(aPacked);
                    }

                    public String pack(Comparable anId) {
                        return anId.toString();
                    }
                }
        );
    }

    private class AttrProducerImpl implements Directory.AttributeProducer {
        public Map<String, String> produce() {
            Map<String, String> myFlattenedRingPosns = new HashMap<String, String>();

            myFlattenedRingPosns.put(RING_MEMBERSHIP,
                    _packager.flattenRingPositions(_ringPositions.get(_peer.getAddress())));

            return myFlattenedRingPosns;
        }
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
                RingPositions myPeerPositions = _packager.extractRingPositions(aNewEntry);

                _logger.debug("New positions from new: " + aNewEntry.getPeerName(), myPeerPositions);

                /*
                 * Slightly naughty as there may be a more up to date version kicking around but that will get
                 * worked out over time
                 */
                _ringPositions.put(aNewEntry.getPeerName(), myPeerPositions);
            }

            for (Directory.Entry anUpdatedEntry : Iterables.filter(anUpdatedPeers, new Predicate<Directory.Entry>() {
                public boolean apply(Directory.Entry entry) {
                    return entry.getAttributes().containsKey(RING_MEMBERSHIP);
                }
            })) {
                RingPositions myPeerPositions = _packager.extractRingPositions(anUpdatedEntry);
                RingPositions myPrevious = _ringPositions.get(anUpdatedEntry.getPeerName());

                // Was the positions list updated?
                //
                if (myPrevious == null) {
                    _logger.debug("New positions from: " + anUpdatedEntry.getPeerName(), myPeerPositions);

                    /*
                     * Slightly naughty as there may be a more up to date version kicking around but that will get
                     * worked out over time
                     */
                    _ringPositions.put(anUpdatedEntry.getPeerName(), myPeerPositions);
                } else {
                    if (myPeerPositions.supercedes(myPrevious)) {
                        _logger.debug("Updated positions from: " + anUpdatedEntry.getPeerName(), myPeerPositions);

                        _ringPositions.replace(anUpdatedEntry.getPeerName(), myPrevious, myPeerPositions);
                    }
                }
            }

            RingRebuild myRingRebuild = rebuildRing(_ringPositions);

            if (! myRingRebuild._rejected.isEmpty()) {
                RingPositions myOldPosns = _ringPositions.get(_peer.getAddress());
                _ringPositions.replace(_peer.getAddress(), myOldPosns, myOldPosns.remove(myRingRebuild._rejected));

                for (RingPosition myPosn : myRingRebuild._rejected) {
                    for (Listener anL : _listeners) {
                        anL.rejected(ConsistentHash.this, myPosn);
                    }
                }
            }

            // No point in a diff if we're empty
            //
            if (myRingRebuild._newRing.isEmpty())
                return;

            /*
             * JVM Bug! Seemingly if rebuildNeighbours does not return two completely independent sets, the following
             * clear and addAll will cause _changes to become empty in spite of the fact that it's possible duplicate
             * myNeighbourRebuild._neighbours is not empty. Another possibility is that a second final field in a simple
             * return object, as done with ring and neighbour computations, causes problems. Notably both methods
             * have required the same treatment to prevent loss of set contents and thus the latter seems more likely.
             * Perhaps something to do with stack scope/corruption?

            _logger.debug(Thread.currentThread() + " " + this + " Rebuild: " + myNeighbourRebuild._neighbours +
                    " " + System.identityHashCode(myNeighbourRebuild._neighbours));
            _logger.debug(Thread.currentThread() + " " + this + " Changes before: " + myNeighbourRebuild._changes +
                    " " + System.identityHashCode(myNeighbourRebuild._changes));
            _logger.debug(Thread.currentThread() + " " + this + " Neighbours before: " + _neighbours +
                    " " + System.identityHashCode(_neighbours));

            _neighbours.clear();

            _logger.debug(Thread.currentThread() + " " + this + " Rebuild after 1: " + myNeighbourRebuild._neighbours +
                    " " + System.identityHashCode(myNeighbourRebuild._neighbours));
            _logger.debug(Thread.currentThread() + " " + this + " Changes after 1: " + myNeighbourRebuild._changes +
                    " " + System.identityHashCode(myNeighbourRebuild._changes));
            _logger.debug(Thread.currentThread() + " " + this + " Neighbours after 1: " + _neighbours +
                    " " + System.identityHashCode(_neighbours));

            _neighbours.addAll(myNeighbourRebuild._neighbours);

            _logger.debug(Thread.currentThread() + " " + this + " Rebuild after 2: " + myNeighbourRebuild._neighbours +
                    " " + System.identityHashCode(myNeighbourRebuild._neighbours));
            _logger.debug(Thread.currentThread() + " " + this + " Changes after 2: " + myNeighbourRebuild._changes +
                    " " + System.identityHashCode(myNeighbourRebuild._changes));
            _logger.debug(Thread.currentThread() + " " + this + " Neighbours after 2: " + _neighbours +
                    " " + System.identityHashCode(_neighbours));
             */

            NeighboursRebuild myNeighbourRebuild =
                    rebuildNeighbours(myRingRebuild._newRing.values(), _neighbours.get(), _peer);

            _neighbours.set(myNeighbourRebuild._neighbours);

            if (! myNeighbourRebuild._changes.isEmpty())
                for (Listener myL : _listeners)
                    for (NeighbourRelation myChange : myNeighbourRebuild._changes)
                        myL.newNeighbour(ConsistentHash.this, myChange._owned, myChange._neighbour);
        }
    }

    private static class RingRebuild {
        final Map<Comparable, RingPosition> _newRing;
        final List<RingPosition> _rejected;

        RingRebuild(Map<Comparable, RingPosition> aNewRing, List<RingPosition> aRejected) {
            _newRing = aNewRing;
            _rejected = aRejected;
        }
    }

    private RingRebuild rebuildRing(Map<String, RingPositions> aRingPositions) {

        /*
         * Re-build the ring from _ringPositions
         *
         * Doing collision resolution as we go. In the case where one of our positions is the loser, remove it
         * and report it to listeners.
         */
        Map<Comparable, RingPosition> myNewRing = new HashMap<Comparable, RingPosition>();
        List<RingPosition> myLocalRejections = new LinkedList<RingPosition>();

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

        // JVM workaround for clear and addAll
        // return new RingRebuild(myNewRing, new LinkedList<RingPosition>(myLocalRejections));

        return new RingRebuild(myNewRing, myLocalRejections);
    }

    private static class NeighboursRebuild {
        final HashSet<NeighbourRelation> _neighbours;
        final Set<NeighbourRelation> _changes;

        NeighboursRebuild(HashSet<NeighbourRelation> aNeighbours, Set<NeighbourRelation> aChanges) {
            _neighbours = aNeighbours;
            _changes = aChanges;
        }
    }

    private NeighboursRebuild rebuildNeighbours(Collection<RingPosition> aRing,
                                                         HashSet<NeighbourRelation> anOldNeighbours,
                                                         Peer aLocal) {
        HashSet<NeighbourRelation> myNeighbours = new HashSet<NeighbourRelation>();
        SortedSet<RingPosition> myRing = new TreeSet<RingPosition>(aRing);
        RingPosition myLast = myRing.last();

        for (RingPosition myPosn : myRing) {
            if (myPosn.isLocal(aLocal) && (! myPosn.equals(myLast))) {
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

        return new NeighboursRebuild(myNeighbours, myChanges);
    }

    public Set<NeighbourRelation> getNeighbours() {
        return Collections.unmodifiableSet(_neighbours.get());
    }

    public Collection<RingPosition> getRing() {
        return Collections.unmodifiableCollection(rebuildRing(_ringPositions)._newRing.values());
    }

    public RingPositions getPeerPositions() {
        return _ringPositions.get(_peer.getAddress());
    }

    RingPosition insertPosition(RingPosition aPosn) {
        RingPositions myOldPosns = _ringPositions.get(_peer.getAddress());
        _ringPositions.replace(_peer.getAddress(), myOldPosns, myOldPosns.add(Collections.singletonList(aPosn)));

        return aPosn;
    }

    private SortedSet<Comparable> flattenPositions() {
        // Simply flatten _ringPositions to get a view of current ring, don't care about peers or collision detection
        //
        TreeSet<Comparable> myOccupiedPositions = new TreeSet<Comparable>();

        for (RingPositions myRPs : _ringPositions.values())
            for (RingPosition myRP : myRPs.getPositions())
                myOccupiedPositions.add(myRP.getPosition());

        return myOccupiedPositions;
    }

    public RingPosition newPosition() {
        SortedSet<Comparable> myOccupiedPositions = flattenPositions();

        Comparable myNewPos;

        do {
            myNewPos = _positionGenerator.newId();
        } while (myOccupiedPositions.contains(myNewPos));

        return insertPosition(new RingPosition(_peer, myNewPos));
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
    public RingPosition allocate(Comparable aHashCode) {
        SortedSet<RingPosition> myPositions = new TreeSet<RingPosition>(rebuildRing(_ringPositions)._newRing.values());

        // If aHashCode is greater than the greatest position, it wraps around to the first
        //
        if (myPositions.last().getPosition().compareTo(aHashCode) < 1)
            return myPositions.first();
        else {
            for (RingPosition myPos : myPositions) {
                if (myPos.getPosition().compareTo(aHashCode) == 1) {
                    return myPos;
                }
            }
        }

        throw new RuntimeException("Impossible but the compiler is too stupid to know");
    }
}
