package org.dancres.peers.acc;

import com.ning.http.client.AsyncHttpClient;
import org.dancres.peers.Peer;
import org.dancres.peers.primitives.HttpServer;
import org.dancres.peers.primitives.InProcessPeer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Example use of DecayingAccumulators
 */
public class LimitTest {
    private static final long PERIOD = 30000;
    private static final long SAMPLE = 5000;

    /**
     * A little propogation delay occurs on updates, allow for that in the window
     */
    private static final long FUDGE = 2000;

    private static final long WINDOW_SIZE = PERIOD - SAMPLE + FUDGE;
    private static final long MAX_REQUESTS_PER_PERIOD = 500000000;

    private static final Logger _logger = LoggerFactory.getLogger(LimitTest.class);
    private HttpServer _server;
    private Peer[] _peers;
    private Total _total;
    private DecayingAccumulators[] _accs;

    @Before
    public void init() throws Exception {
        _server = new HttpServer(new InetSocketAddress("localhost", 8081));
        AsyncHttpClient myClient = new AsyncHttpClient();

        _peers = new Peer[3];
        _accs = new DecayingAccumulators[3];

        for (int i = 0; i < _peers.length; i++) {
            _peers[i] = new InProcessPeer(_server, myClient, "/peer" + Integer.toString(i), new Timer());
            _accs[i] = new DecayingAccumulators(_peers[i], WINDOW_SIZE);
            _peers[i].add(_accs[i]);
        }

        _total = new Total(MAX_REQUESTS_PER_PERIOD);

        // Use the local peer's timer to schedule our count updates
        //
        _peers[0].getTimer().schedule(new Snapshotter(), 0, SAMPLE);
    }

    @After
    public void deInit() throws Exception {
        for (int i = 0; i < _peers.length; i++)
            _peers[i].stop();

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
                _logger.info("Contributing: " + mySample);

                SortedSet<DecayingAccumulators.Count> myTotals = new TreeSet<>();

                for (int i = 0; i < _peers.length; i++) {
                    String myPeer = _peers[i].getAddress();

                    // Use the local peer to send updates out to this peer and siblings
                    //
                    myTotals.add(_accs[0].log(myPeer, mySample));
                }

                _logger.info("New total: " + myTotals.last());

                _total.update(myTotals.last());
            } catch (Exception anE) {
                _logger.error("Couldn't log count", anE);
            }
        }
    }
}