package org.dancres.peers;

import org.dancres.peers.messaging.Channels;
import org.dancres.util.Tuple;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class ChannelsTest {
    @Test
    public void doDiff() {
        Set<String> myLocal = new HashSet<>();
        Set<String> myRemote = new HashSet<>();
        List<String> myExpectedLocals = new LinkedList<>();
        List<String> myExpectedRemotes = new LinkedList<>();

        Collections.addAll(myLocal, "1", "3", "5");
        Collections.addAll(myRemote, "2", "3", "4");
        Collections.addAll(myExpectedLocals, "2", "4");
        Collections.addAll(myExpectedRemotes, "1", "5");

        Tuple<Set<String>, Set<String>> myDiffs = Channels.diff(myLocal, myRemote);

        Assert.assertEquals(2, myDiffs.getFirst().size());
        Assert.assertEquals(2, myDiffs.getSecond().size());
        Assert.assertTrue(myDiffs.getFirst().containsAll(myExpectedLocals));
        Assert.assertTrue(myDiffs.getSecond().containsAll(myExpectedRemotes));
    }
}
