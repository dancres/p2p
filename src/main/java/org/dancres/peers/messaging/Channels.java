package org.dancres.peers.messaging;

import com.google.common.collect.Sets;
import org.dancres.util.Tuple;

import java.util.Set;

public class Channels {
    /**
     * Yield the identifiers of <code>Channel</code>'s that are known locally but not remotely and those that are known remotely
     * but not locally.
     *
     * @param aLocal is the set of known local <code>Channel</code>'s
     * @param aRemote is the set of known remote <code>Channel</code>'s
     * @return a Tuple containing those channel ids not known locally and those not known remotely
     */
    public static Tuple<Set<String>, Set<String>> diff(Set<String> aLocal, Set<String> aRemote) {
        Set<String> myNotKnownRemotely = Sets.difference(aLocal, aRemote).immutableCopy();
        Set<String> myNotKnownLocally = Sets.difference(aRemote, aLocal).immutableCopy();

        return new Tuple<>(myNotKnownLocally, myNotKnownRemotely);
    }
}
