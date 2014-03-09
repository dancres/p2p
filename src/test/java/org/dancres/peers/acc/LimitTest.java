package org.dancres.peers.acc;

import com.ning.http.client.AsyncHttpClient;
import org.dancres.peers.Directory;
import org.dancres.peers.Peer;
import org.dancres.peers.PeerSet;
import org.dancres.peers.primitives.GossipBarrier;
import org.dancres.peers.primitives.HttpServer;
import org.dancres.peers.primitives.InProcessPeer;
import org.dancres.peers.primitives.StaticPeerSet;
import org.dancres.peers.ring.ConsistentHash;
import org.dancres.peers.ring.RingPosition;
import org.dancres.peers.ring.StabiliserImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Example use of DecayingAccumulators
 */
public class LimitTest {
    private static final int MAX_PEERS = 3;
    private static final long PERIOD = 30000;
    private static final long SAMPLE = 5000;

    /**
     * A little propogation delay occurs on updates, allow for that in the window
     */
    private static final long FUDGE = 2000;

    private static final long WINDOW_SIZE = PERIOD - SAMPLE + FUDGE;
    private static final long MAX_REQUESTS_PER_PERIOD = 500000000;

    private static final Logger _logger = LoggerFactory.getLogger(LimitTest.class);

    private final LinkedList<Peer> _peers = new LinkedList<>();
    private final LinkedList<ConsistentHash> _hashes = new LinkedList<>();
    private final LinkedList<GossipBarrier> _barriers = new LinkedList<>();
    private final LinkedList<DecayingAccumulators> _accs = new LinkedList<>();
    private final Total _total = new Total(MAX_REQUESTS_PER_PERIOD);

    private HttpServer _server;

    @Before
    public void init() throws Exception {
        _server = new HttpServer(new InetSocketAddress("localhost", 8081));
        AsyncHttpClient myClient = new AsyncHttpClient();

        _logger.info("Peers and Accs");

        for (int i = 0; i < MAX_PEERS; i++) {
            Peer myPeer = new InProcessPeer(_server, myClient, "/peer" + Integer.toString(i), new Timer());

            _peers.add(myPeer);
            _accs.add(new DecayingAccumulators(myPeer, WINDOW_SIZE));
        }

        Set<URI> myPeers = new HashSet<>();

        for (Peer myPeer : _peers) {
            myPeers.add(myPeer.getURI());
        }

        PeerSet myPeerSet = new StaticPeerSet(myPeers);

        _logger.info("Directories");

        // Now we have some peers, get directories and barriers up
        //
        for (Peer myPeer : _peers) {
            Directory myDir = new Directory(myPeer, myPeerSet, 1000, 5000);
            GossipBarrier myBarrier = new GossipBarrier(myDir);

            _barriers.add(myBarrier);

            myDir.start();
        }

        _logger.info("Hash Rings");

        // Dirs are up, now consistent hash rings and positions
        //
        for (Peer myPeer : _peers) {
            ConsistentHash myHash = new ConsistentHash(myPeer);

            _hashes.add(myHash);
            myHash.add(new StabiliserImpl());

            for (int j = 0; j < 3; j++) {
                myHash.createPosition();
            }
        }

        _logger.info("Stability?");

        // Now wait for things to settle
        //
        boolean amStable = false;

        for (int i = 0; i < 10; i++) {
            for (GossipBarrier myBarrier : _barriers) {
                myBarrier.await(myBarrier.current());
            }

            amStable = true;
            for (ConsistentHash myHash : _hashes) {
                int myHashSize = myHash.getRing().getPositions().size();

                if (myHashSize != 9) {
                    _logger.info("Ring " + myHash + " is currently at " + myHashSize + " need 9");
                    amStable = false;
                }
            }

            if (amStable)
                break;
        }

        if (! amStable)
            throw new RuntimeException("Never got stable");

        _logger.info("Running with hashring: " + _hashes.getFirst().getRing().getPositions());

        // Use the local peer's timer to schedule our count updates
        //
        _peers.getFirst().getTimer().schedule(new Snapshotter(), 0, SAMPLE);
    }

    @After
    public void deInit() throws Exception {
        for (Peer myPeer : _peers)
            myPeer.stop();

        _server.terminate();
    }

    @Test
    public void countAndLimit() throws Exception {
        long myStopTime = System.currentTimeMillis() + (PERIOD * 5);

        while (myStopTime > System.currentTimeMillis()) {
            if (_total.increment()) {
                _logger.info("*** Choke: " + _total.currentCount() + " ***");

                // Reject or whatever - for demonstration, we'll just pause
                //
                Thread.sleep(PERIOD / 2);

                _logger.info("*** Unchoke: " + _total.currentCount() + " ***");
            }
        }

        _logger.info("Exiting");
    }

    /**
     * Total is samples gathered in WINDOW_SIZE + whatever else we see locally in the next SAMPLE milliseconds
     */
    private static class Total {
        private final AtomicLong _base = new AtomicLong(0);
        private final AtomicLong _increment = new AtomicLong(0);
        private final long _limit;

        Total(long aLimit) {
            _limit = aLimit;
        }

        boolean increment() {
            long myTotal = _increment.incrementAndGet() + _base.get();

            return (myTotal >= _limit);
        }

        long currentCount() {
            return _base.get() + _increment.get();
        }

        DecayingAccumulators.Count contribute() {
            long myTotal = _increment.get();
            _increment.set(0);
            return new DecayingAccumulators.Count("a", SAMPLE, myTotal);
        }

        long update(DecayingAccumulators.Count aCount) {
            _base.set(aCount.getCount());

            return currentCount();
        }
    }

    private class Snapshotter extends TimerTask {
        public void run() {
            try {
                DecayingAccumulators.Count mySample = _total.contribute();

                // Use the local hash ring to identify servers to use
                //
                Integer myHash = mySample.getAccumulatorId().hashCode();
                List<RingPosition> myPositions = _hashes.getFirst().getRing().allocate(myHash, 3);
                Set<String> myPeers = new HashSet<>();

                // We do nothing to enforce a properly balanced hash ring so could end up with the same
                // node more than once. We don't want to send the same sample more than once to any given peer
                // otherwise we get duplicate counts and incorrect totals. So, de-dupe the peers...
                //
                for (RingPosition myPos: myPositions) {
                    myPeers.add(myPos.getPeerAddress());
                }

                _logger.info("Contributing " + mySample + " to " + myPeers);

                SortedSet<DecayingAccumulators.Count> myTotals = new TreeSet<>();

                for (String myPeer: myPeers) {
                    // Use the local peer to send updates out to this peer and siblings
                    //
                    myTotals.add(_accs.getFirst().log(myPeer, mySample));
                }

                _logger.info("New total: " + myTotals.last());

                _total.update(myTotals.last());
            } catch (Exception anE) {
                _logger.error("Couldn't log count", anE);
            }
        }
    }
}
