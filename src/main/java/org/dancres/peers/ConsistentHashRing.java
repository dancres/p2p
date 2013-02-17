package org.dancres.peers;

/**
 * Bucket leasing, bucket birth dates etc
 *
 * Allocate ourselves some set of buckets, each with an id (say a random integer which fits nicely with hashCode()).
 *
 * Each bucket has a birth date which we can use to resolve collisions.
 *
 * As each node in the ring can perform the hash and determine the bucket to assign it a uniquely identified
 * (UUID or String or ?) value to, any client can write to any node although it would be best if the client cached
 * the map itself (implement this later). We will store the key, value and the key hashcode.
 *
 * The Directory will announce the comings and goings of nodes and we will interrogate new or updated nodes for their
 * buckets, dumping any they were previously associated with.
 *
 * Should we detect a bucket that clashes with our locally allocated buckets we first compare birth dates and the oldest
 * one wins. If that doesn't work, we consider peer ids and chose a winner from that (lexically or maybe hashCode
 * comparison).
 *
 * Once a winner is determined, the loser is re-numbered randomly and then a job is kicked off to migrate any values to
 * the winner bucket (those with a key that hashes to an appropriate value).
 *
 * A similar value migration strategy is adopted when we detect a new bucket that is adjacent (that is it is less
 * than ours but greater than all others less than ours) to ours. We kick a job off to migrate any values to this
 * new bucket.
 *
 * Buckets can be maintained by a different service within the peer and it will support a migrate function that moves
 * values between buckets.
 *
 * "Each data item identified by a key is assigned to a node by hashing the data item’s key to yield its position on
 * the ring, and then walking the ring clockwise to find the first node with a position larger than the item’s
 * position."
 */
public class ConsistentHashRing {
    public static class ContainerRef {
        public Object getId() {
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
    public ContainerRef allocate(int aHashCode) {
        throw new UnsupportedOperationException();
    }

    /**
     * Add a new id for a new resource container to the hash ring. This will be given a birth-date and published.
     *
     * @param aContainerId
     */
    public void addContainer(Object aContainerId) {
        throw new UnsupportedOperationException();
    }

    public static interface Listener {
        /**
         * Invoked to indicate that resources associated with <code>aSourceNodeId</code> should be moved if a call
         * to ConsistentHashing.allocate indicates the current ContainerId is no longer the most appropriate.
         *
         * @param aSourceNodeId
         * @param aDest
         */
        public void migrate(Object aSourceNodeId, ContainerRef aDest);

        /**
         * Invoked to indicate that the local resource holder with id <code>aConflictingNodeId</code> needs renaming
         * as it conflicts with another elsewhere and has been judged the loser.
         *
         * @param aConflictingRef
         *
         * @return a new NodeId
         */
        public Object resolveCollision(ContainerRef aConflictingRef);
    }
}
