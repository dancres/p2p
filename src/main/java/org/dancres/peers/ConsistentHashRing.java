package org.dancres.peers;

import java.util.List;

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
 */
public class ConsistentHashRing {
    public ConsistentHashRing(List<Integer> aRingPositions) {
    }

    public static class ContainerRef {
        public Integer getRingPosition() {
            throw new UnsupportedOperationException();
        }

        public String getPeerId() {
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
    public ContainerRef allocate(Integer aHashCode) {
        throw new UnsupportedOperationException();
    }

    public static interface Listener {
        /**
         * Invoked to indicate that resources associated with <code>aRingPosition</code> should be moved if a call
         * to ConsistentHashing.allocate indicates the current ContainerId is no longer the most appropriate.
         *
         * @param aRingPosition
         * @param aDest
         */
        public void migrate(Integer aRingPosition, ContainerRef aDest);

        /**
         * Invoked to indicate that the local resource holder with id <code>aConflictingNodeId</code> needs renaming
         * as it conflicts with another elsewhere and has been judged the loser.
         *
         * @param aConflictingRef
         *
         * @return a new ring position
         */
        public Integer reallocate(ContainerRef aConflictingRef);
    }
}
