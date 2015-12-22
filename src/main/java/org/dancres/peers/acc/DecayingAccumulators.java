package org.dancres.peers.acc;

import com.google.gson.Gson;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import org.dancres.peers.Peer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>A service which maintains a number of independent, uniquely named accumulators.</p>
 *
 * <p>An individual accumulator provides a total for a set of counts within a rolling time window. Thus counts
 * submitted prior to the window are dropped from the total such that if no samples are received for long enough,
 * the total will decay to zero.</p>
 */
public class DecayingAccumulators implements Peer.Service {
    private static final Logger _logger = LoggerFactory.getLogger(DecayingAccumulators.class);

    /**
     * The protocol for DecayingAccumulators is GET to read a total for an accumulator and POST to add a count to
     * an accumulator. The protocol isn't RESTful as for the POST, the count id is passed inside of the body
     * of the post as part of a GSON'd Count instance. The GET passes the id of the desired accumulator as a query
     * parameter and would require. In both cases, one would prefer to pass the id as part of the URL path.
     */
    private class Dispatcher implements Peer.ServiceDispatcher {
        public void dispatch(String aServicePath, HttpRequest aRequest, HttpResponse aResponse) {
            if (aRequest.getMethod().equals(HttpMethod.POST)) {
                String mySampleString = aRequest.getContent().toString(CharsetUtil.UTF_8);

                Gson myGson = new Gson();
                Count myCount = myGson.fromJson(mySampleString, Count.class);

                Count myAccumulation = add(myCount);

                aResponse.setContent(ChannelBuffers.copiedBuffer(myGson.toJson(myAccumulation), CharsetUtil.UTF_8));
                aResponse.setStatus(HttpResponseStatus.OK);
            } else if (aRequest.getMethod().equals(HttpMethod.GET)){
                String myIdString = new QueryStringDecoder(aRequest.getUri()).getParameters().get("id").get(0);

                Gson myGson = new Gson();
                String myId = myGson.fromJson(myIdString, String.class);

                Count myAccumulation = reduce(myId);

                aResponse.setContent(ChannelBuffers.copiedBuffer(myGson.toJson(myAccumulation), CharsetUtil.UTF_8));
                aResponse.setStatus(HttpResponseStatus.OK);
            } else {
                aResponse.setStatus(HttpResponseStatus.BAD_REQUEST);
            }
        }
    }

    public static class Count implements Comparable<Count> {
        private final String _accumulatorId;
        private final String _nonce;
        private final long _samplePeriodInMillis;
        private final long _count;

        /**
         * @param anAccumulatorId is the id of the accumulator to add this count to
         * @param aSamplePeriod is the period of which the count was taken
         * @param aCount is the count itself
         * @param aNonce uniquely identifies a count for purposes of de-duping (e.g. because of message-loss)
         */
        Count(String anAccumulatorId, long aSamplePeriod, long aCount, String aNonce) {
            _accumulatorId = anAccumulatorId;
            _samplePeriodInMillis = aSamplePeriod;
            _count = aCount;
            _nonce = aNonce;
        }

        public String getAccumulatorId() {
            return _accumulatorId;
        }

        public long getSamplePeriod() {
            return _samplePeriodInMillis;
        }

        public long getCount() {
            return _count;
        }

        public String getNonce() { return _nonce; }

        public String toString() {
            return "ID: " + _accumulatorId + ", Period: " + _samplePeriodInMillis + ", Count: " + _count +
                    ", Nonce: " + _nonce;
        }

        public int compareTo(Count aCount) {
            if (_count < aCount._count)
                return -1;
            else if (_count > aCount._count)
                return 1;
            else
                return 0;
        }
    }

    private class Docket {
        private final long _arrivalTime;
        private final Count _count;

        Docket(Count aCount) {
            _arrivalTime = System.currentTimeMillis();
            _count = aCount;
        }

        public boolean isCurrent(long aMinimumAge) {
            return ((_arrivalTime - _count.getSamplePeriod()) >= aMinimumAge);
        }

        public boolean equals(Object anObject) {
            if (anObject instanceof Docket) {
                Docket myOther = (Docket) anObject;

                return ((myOther._count.getAccumulatorId().equals(_count.getAccumulatorId())) &&
                        (myOther._count.getNonce().equals(_count.getNonce())));
            }

            return false;
        }

        public Count getSample() {
            return _count;
        }

        public int hashCode() {
            return _count.getAccumulatorId().hashCode() ^ _count.getNonce().hashCode();
        }
    }

    private final Peer.ServiceDispatcher _dispatcher;
    private final ConcurrentHashMap<String, Set<Docket>> _collectedSamples = new ConcurrentHashMap<>();
    private final Peer _peer;
    private final long _window;

    /**
     * Use this method to setup a client or a server with a default window of 60 seconds on the specified peer.
     * (invokes <code>Peer.add</code> at construction).
     *
     * @param aPeer to bind this service to
     */
    public DecayingAccumulators(Peer aPeer) {
        this(aPeer, 60000);
    }

    /**
     * Use this method to setup a client or a server with a specified window on the specified peer.
     * (invokes <code>Peer.add</code> at construction).
     *
     * @param aPeer to bind this service to
     * @param aQuantum the window over which counts are accumulated in milliseconds
     */
    public DecayingAccumulators(Peer aPeer, long aQuantum) {
        _peer = aPeer;
        _dispatcher = new Dispatcher();
        _window = aQuantum;
        _peer.add(this);
    }

    public Peer.ServiceDispatcher getDispatcher() {
        return _dispatcher;
    }

    public String getAddress() {
        return "/rc";
    }

    /**
     * Construct a new Count object to be logged with one or more Peers.
     *
     * @param anAccumulatorId the id of the accumulator we wish to add a count to
     * @param aSamplePeriod the period over which the count was taken
     * @param aCount the count for the period
     * @return
     */
    public Count newCount(String anAccumulatorId, long aSamplePeriod, long aCount) {
        UUID myNonce = UUID.randomUUID();
        return new Count(anAccumulatorId, aSamplePeriod, aCount, myNonce.toString());
    }

    /**
     * Log a Count at a specified peer.
     *
     * @param aPeerAddress is the peer maintaining the accumulator
     * @param aCount is the sample to add
     * @return the total of all samples received for the accumulator
     * @throws Exception
     */
    public Count log(String aPeerAddress, Count aCount) throws Exception {
        final Gson myGson = new Gson();
        AsyncHttpClient myClient = _peer.getClient();

        Future<Count> mySample = myClient.preparePost(aPeerAddress + getAddress()).setBody(
                myGson.toJson(aCount)).execute(new AsyncCompletionHandler<Count>() {
            public Count onCompleted(Response aResponse) throws Exception {
                String myTotalSample = aResponse.getResponseBody();

                try {
                    return myGson.fromJson(myTotalSample, Count.class);
                } catch (Exception anE) {
                    _logger.error("Error in unpack", anE);
                    throw anE;
                }
            }
        });

        return mySample.get();
    }

    /**
     * Get the current total of all Counts received from the specified peer
     *
     * @param aPeerAddress is the peer maintaining the counter
     * @param anId is the name of the accumulator for which a total is required
     * @return the current total
     * @throws Exception
     */
    public Count get(String aPeerAddress, String anId) throws Exception {
        final Gson myGson = new Gson();
        AsyncHttpClient myClient = _peer.getClient();

        Future<Count> mySample = myClient.prepareGet(aPeerAddress + getAddress()).addQueryParameter("id",
                myGson.toJson(anId)).execute(new AsyncCompletionHandler<Count>() {
            public Count onCompleted(Response aResponse) throws Exception {
                String myTotalSample = aResponse.getResponseBody();

                try {
                    return myGson.fromJson(myTotalSample, Count.class);
                } catch (Exception anE) {
                    _logger.error("Error in unpack", anE);
                    throw anE;
                }
            }
        });

        return mySample.get();
    }

    /*
     * These two methods maintain immutable lists of samples such that if something is added or removed, a new list
     * is created and placed in the ConcurrentMap.
     *
     * The opportunity to remove an empty list from the ConcurrentMap is taken within reduce() exploiting the fact
     * that concurrent updates perform testAndSet replace or remove. Winners cause losers to retry their updates in the
     * case of add() or just give up in the case of reduce() (because the work can be performed again later without
     * much harm).
     */
    private Count add(Count aCount) {
        String myId = aCount.getAccumulatorId();

        while (true) {
            Set<Docket> mySamples = _collectedSamples.get(myId);

            if (mySamples == null) {
                Set<Docket> myInitial = new HashSet<>();
                myInitial.add(new Docket(aCount));
                Set<Docket> myResult = _collectedSamples.putIfAbsent(myId, Collections.unmodifiableSet(myInitial));

                if (myResult == null)
                    break;
            } else {
                Set<Docket> myReplace = new HashSet<>(mySamples);
                myReplace.add(new Docket(aCount));

                if (_collectedSamples.replace(myId, mySamples, Collections.unmodifiableSet(myReplace)))
                    break;
            }
        }

        return reduce(aCount.getAccumulatorId());
    }

    private Count reduce(String anId) {
        Set<Docket> myDockets = _collectedSamples.get(anId);

        if (myDockets == null)
            return newCount(anId, _window, 0);
        else {
            Set<Docket> myReduced = new HashSet<>(myDockets);

            long myMinimumAge = System.currentTimeMillis() - _window;
            long myTotal = 0;

            Iterator<Docket> myCurrentSamples = myReduced.iterator();
            while (myCurrentSamples.hasNext()) {
                Docket myDocket = myCurrentSamples.next();

                if (myDocket.isCurrent(myMinimumAge)) {
                    myTotal += myDocket.getSample().getCount();
                } else {
                    myCurrentSamples.remove();
                }
            }

            // Either of these can fail, that's fine, it means someone else has done a clean via reduce or add
            //
            if (myReduced.size() == 0)
                _collectedSamples.remove(anId, myDockets);
            else
                _collectedSamples.replace(anId, myDockets, Collections.unmodifiableSet(myReduced));


            return newCount(anId, _window, myTotal);
        }
    }
}
