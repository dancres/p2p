package org.dancres.peers;

import com.google.gson.Gson;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

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
 */
public class ConsistentHashRing {
    private static final String RING_MEMBERSHIP = "org.dancres.peers.consistentHashRing.ringMembership";

    private final Peer _peer;
    private final Directory _dir;
    private final AtomicLong _membershipGeneration = new AtomicLong(0);
    private final Random _rng = new Random();
    private final Set<RingPosition> _ownedPositions;
    private final Set<RingPosition> _allPositions;

    public static class RingPosition implements Comparable {
        private String _peerName;
        private Integer _position;
        private long _birthDate;

        public RingPosition(String aPeerName, Integer aPosition) {
            this(aPeerName, aPosition, System.currentTimeMillis());
        }

        RingPosition(String aPeerName, Integer aPosition, long aBirthDate) {
            _peerName = aPeerName;
            _position = aPosition;
            _birthDate = aBirthDate;
        }

        public int compareTo(Object anObject) {
            RingPosition myOther = (RingPosition) anObject;

            return (_position.intValue() - myOther._position.intValue());
        }
    }

    public ConsistentHashRing(Peer aPeer, Directory aDirectory) {
        _peer = aPeer;
        _dir = aDirectory;
        _ownedPositions = new TreeSet<RingPosition>();
        _allPositions = new TreeSet<RingPosition>();

        _dir.add(new AttrProducerImpl());
    }

    public RingPosition newPosition() {
        RingPosition myNewPos;

        do {
            myNewPos = new RingPosition(_peer.getAddress().toString(), _rng.nextInt());
        } while ((_ownedPositions.contains(myNewPos)) || (_allPositions.contains(myNewPos)));

        _ownedPositions.add(myNewPos);
        _allPositions.add(myNewPos);

        _membershipGeneration.incrementAndGet();

        return myNewPos;
    }

    private class AttrProducerImpl implements Directory.AttributeProducer {
        public Map<String, String> produce() {
            throw new UnsupportedOperationException();
        }
    }

    public void add(Listener aListener) {
        throw new UnsupportedOperationException();
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
