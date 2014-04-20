package org.dancres.peers.ring;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.dancres.peers.Directory;
import org.dancres.peers.Peer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consistent Hash Ring implementation.
 *
 * <p>Each hash ring requires a unique name and there may be several hash rings active on any given peer.</p>
 *
 * <p>Members of a hash ring needn't be servers i.e. maintain a set of positions, they can just be clients that use
 * the hash ring for whatever purpose is intended.</p>
 *
 * <p><b>Note:</b> This implementation is dependent upon a <code>Directory</code> service having been instantiated on
 * the peer previously.</p>
 */
public class ConsistentHash<T extends Comparable> {
    private static final String RING_MEMBERSHIP_BASE = "org.dancres.peers.ring.consistentHash.ringMembership";

    private static final Logger _logger = LoggerFactory.getLogger(ConsistentHash.class);

    /**
     * Responsible for creating candidate positions on a ring, collision detection is done in the hash ring
     * implementation.
     */
    public interface PositionGenerator<T> {
        public T newId();
    }

    /**
     * Responsible for marshalling and unmarshalling positions produced by an associated generator.
     */
    public interface PositionPacker<T> {
        public T unpack(String aPacked);
        public String pack(T anId);
    }

    public interface Listener<T extends Comparable> {
        public void changed(RingSnapshot<T> aSnapshot);
        public void rejected(ConsistentHash<T> aRing, RingPosition anOwnedPosition);
    }

    private final Peer _peer;
    private final List<Listener<T>> _listeners = new CopyOnWriteArrayList<>();

    /**
     * The positions held by each node identified by address
     */
    private final ConcurrentMap<String, RingPositions<T>> _ringPositions = new ConcurrentHashMap<>();

    private final Packager<T> _packager;
    private final PositionGenerator<T> _positionGenerator;
    private final String _ringName;

    /**
     * Create a ring on a peer with a specified name and using positions created by a generator and marshalled via
     * a packer. The generator and packer must be correctly paired together.
     *
     * @param aPeer
     * @param aGenerator
     * @param aPacker
     * @param aRingName
     *
     * @throws RuntimeException if there is no <code>Directory</code> service registered on the specified peer
     */
    public ConsistentHash(Peer aPeer, PositionGenerator<T> aGenerator, PositionPacker<T> aPacker, String aRingName) {
        if (aRingName == null)
            throw new IllegalArgumentException("Name cannot be null");

        if (aGenerator == null)
            throw new IllegalArgumentException("Generator cannot be null");

        if (aPacker == null)
            throw new IllegalArgumentException("Packer cannot be null");

        _ringName = RING_MEMBERSHIP_BASE + "." + aRingName;
        _peer = aPeer;
        _positionGenerator = aGenerator;
        _packager = new Packager<>(aPacker, _ringName);
        Directory myDir = (Directory) aPeer.find(Directory.class);

        if (myDir == null)
            throw new RuntimeException("ConsistentHash couldn't locate a Directory service in peer");

        _ringPositions.put(_peer.getAddress(), new RingPositions<T>());

        myDir.add(new AttrProducerImpl());
        myDir.add(new DirListenerImpl());
    }

    public static ConsistentHash<Integer> createRing(Peer aPeer) {
        return createRing(aPeer, "DefaultRing");
    }

    public static ConsistentHash<Integer> createRing(Peer aPeer, String aRingName) {
        return new ConsistentHash<>(aPeer,
                new PositionGenerator<Integer>() {
                    private Random _rng = new Random();

                    public Integer newId() {
                        return _rng.nextInt();
                    }
                },
                new PositionPacker<Integer>() {
                    public Integer unpack(String aPacked) {
                        return Integer.parseInt(aPacked);
                    }

                    public String pack(Integer anId) {
                        return anId.toString();
                    }
                },
                aRingName);

    }

    private class AttrProducerImpl implements Directory.AttributeProducer {
        public Map<String, String> produce() {
            Map<String, String> myFlattenedRingPosns = new HashMap<>();

            myFlattenedRingPosns.put(_ringName,
                    _packager.flattenRingPositions(_ringPositions.get(_peer.getAddress())));

            return myFlattenedRingPosns;
        }
    }

    private class DirListenerImpl implements Directory.Listener {
        public void updated(Directory aDirectory, List<Directory.Entry> aNewPeers,
                            List<Directory.Entry> anUpdatedPeers, List<Directory.Entry> aDeadPeers) {

            _logger.debug("Ring Update");

            boolean haveUpdates = false;

            // Extract implicitly new positions from newly discovered peer
            //
            for (Directory.Entry aNewEntry : Iterables.filter(aNewPeers, new Predicate<Directory.Entry>() {
                public boolean apply(Directory.Entry entry) {
                    return entry.getAttributes().containsKey(_ringName);
                }
            })) {
                RingPositions<T> myPeerPositions = _packager.extractRingPositions(aNewEntry);

                _logger.debug("New positions from new: " + aNewEntry.getPeerName(), myPeerPositions);

                /*
                 * Slightly naughty as there may be a more up to date version kicking around but that will get
                 * worked out over time
                 */
                _ringPositions.put(aNewEntry.getPeerName(), myPeerPositions);
                haveUpdates = true;
            }

            // For updated peers, if they're a ring member that just acquired their first set of positions from our
            // perspective, treat them as new, otherwise replace the existing ones
            //
            for (Directory.Entry anUpdatedEntry : Iterables.filter(anUpdatedPeers, new Predicate<Directory.Entry>() {
                public boolean apply(Directory.Entry entry) {
                    return entry.getAttributes().containsKey(_ringName);
                }
            })) {
                RingPositions<T> myPeerPositions = _packager.extractRingPositions(anUpdatedEntry);
                RingPositions<T> myPrevious = _ringPositions.get(anUpdatedEntry.getPeerName());

                // Was the positions list updated?
                //
                if (myPrevious == null) {
                    _logger.debug("New positions from: " + anUpdatedEntry.getPeerName(), myPeerPositions);

                    /*
                     * Slightly naughty as there may be a more up to date version kicking around but that will get
                     * worked out over time
                     */
                    _ringPositions.put(anUpdatedEntry.getPeerName(), myPeerPositions);
                    haveUpdates = true;
                } else {
                    if (myPeerPositions.supercedes(myPrevious)) {
                        _logger.debug("Updated positions from: " + anUpdatedEntry.getPeerName(), myPeerPositions);

                        _ringPositions.replace(anUpdatedEntry.getPeerName(), myPrevious, myPeerPositions);
                        haveUpdates = true;
                    }
                }
            }

            // For dead peers, remove their positions
            //
            for (Directory.Entry aDeadEntry : Iterables.filter(aDeadPeers, new Predicate<Directory.Entry>() {
                public boolean apply(Directory.Entry entry) {
                    return entry.getAttributes().containsKey(_ringName);
                }
            })) {
                RingPositions<T> myPeerPositions = _packager.extractRingPositions(aDeadEntry);

                _logger.debug("Dead positions from: " + aDeadEntry.getPeerName(), myPeerPositions);

                _ringPositions.remove(aDeadEntry.getPeerName());
                haveUpdates = true;
            }

            if (!haveUpdates)
                return;

            // Now recompute the positions on the ring
            //
            RingSnapshot<T> myRingSnapshot = new RingSnapshot<>(_ringPositions, _peer);

            // Signal a general change
            //
            for (Listener<T> anL : _listeners) {
                anL.changed(myRingSnapshot);
            }

            // Clear out the rejections and signal them to listeners
            //
            if (!myRingSnapshot._rejected.isEmpty()) {
                RingPositions<T> myOldPosns = _ringPositions.get(_peer.getAddress());
                _ringPositions.replace(_peer.getAddress(), myOldPosns, myOldPosns.remove(myRingSnapshot._rejected));

                for (RingPosition myPosn : myRingSnapshot._rejected) {
                    for (Listener<T> anL : _listeners) {
                        anL.rejected(ConsistentHash.this, myPosn);
                    }
                }
            }
        }
    }

    private RingPosition insertPosition(RingPosition<T> aPosn) {
        RingPositions<T> myOldPosns = _ringPositions.get(_peer.getAddress());
        _ringPositions.replace(_peer.getAddress(), myOldPosns, myOldPosns.add(Collections.singletonList(aPosn)));

        return aPosn;
    }

    private SortedSet<T> flattenPositions() {
        // Simply flatten _ringPositions to get a view of current ring, don't care about peers or collision detection
        //
        TreeSet<T> myOccupiedPositions = new TreeSet<>();

        for (RingPositions<T> myRPs : _ringPositions.values())
            for (RingPosition<T> myRP : myRPs.getPositions())
                myOccupiedPositions.add(myRP.getPosition());

        return myOccupiedPositions;
    }

    public RingSnapshot<T> getRing() {
        return new RingSnapshot<>(_ringPositions, _peer);
    }

    /**
     * @return the positions occupied by the local peer
     */
    public RingPositions getPeerPositions() {
        return _ringPositions.get(_peer.getAddress());
    }

    /**
     * Create an additional position on the ring for this peer
     *
     * @return The new ring position
     *
     * @throws CollisionException if the operation conflicts with other allocations
     */
    public RingPosition createPosition() throws CollisionException {
        return createPosition(null);
    }

    /**
     * Create an additional position on the ring
     *
     * @param aDesiredPosition Pass <code>null</code> for an automatic assignment or a suitable <code>Comparable</code>
     *                         to request a specific position.
     * @return The new ring position
     *
     * @throws CollisionException if the specified position is already allocated.
     */
    public RingPosition createPosition(T aDesiredPosition) throws CollisionException {
        SortedSet<T> myOccupiedPositions = flattenPositions();

        T myNewPos = aDesiredPosition;

        if (myNewPos != null) {
            if (myOccupiedPositions.contains(myNewPos))
                throw new CollisionException("Desired position is already occupied: " + myNewPos);
        } else {
            do {
                myNewPos = _positionGenerator.newId();
            } while (myOccupiedPositions.contains(myNewPos));
        }

        return insertPosition(new RingPosition<>(_peer, myNewPos));
    }

    public void add(Listener aListener) {
        _listeners.add(aListener);
    }
}
