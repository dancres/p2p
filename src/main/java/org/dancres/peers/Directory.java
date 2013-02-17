package org.dancres.peers;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Directory service that lives atop a set of peers. The set of peers are used to maintain the directory via gossip.
 * There may be many more peers that are part of the directory service though not used to track/maintain it directly.
 *
 * A directory service tracks information across all known peers. The information provided is entirely user-defined
 * via attributes. Liveness tests are supported via timestamps, versioning of attributes is encouraged.
 *
 * @todo Add support for dead node elimination
 */
public class Directory {
    private static final Logger _logger = LoggerFactory.getLogger(Directory.class);
    private final PeerSet _peers;
    private final Peer _peer;
    private final Peer.ServiceDispatcher _dispatcher;
    private final long _birthTime = System.currentTimeMillis();
    private final List<AttributeProducer> _producers = new CopyOnWriteArrayList<AttributeProducer>();
    private final List<Listener> _listeners = new CopyOnWriteArrayList<Listener>();

    private final Executor _notifier = Executors.newSingleThreadExecutor(new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread myDaemon = new Thread(r);

            myDaemon.setDaemon(true);
            return myDaemon;
        }
    });

    public static class Entry {
        private final String _peerName;
        private final Map<String, String> _attributes;
        private final long _born;
        private final long _timestamp;

        Entry(String aName, Map<String, String> anAttrs, long aTimestamp, long aBorn) {
            _peerName = aName;
            _attributes = anAttrs;
            _timestamp = aTimestamp;
            _born = aBorn;
        }

        public String getPeerName() {
            return _peerName;
        }

        public long getTimestamp() {
            return _timestamp;
        }

        public Map<String, String> getAttributes() {
            return _attributes;
        }

        public long getBorn() {
            return _born;
        }

        public boolean equals(Object anObject) {
            if (anObject instanceof Entry) {
                Entry myOther = (Entry) anObject;

                return ((_peerName.equals(myOther.getPeerName())) &&
                        (_timestamp == myOther.getTimestamp()));
            }

            return false;
        }

        public String toString() {
            return "Directory.Entry: " + _peerName +
                    " born: " + _born + " tstamp: " + _timestamp +
                    " attributes:" + _attributes;
        }
    }

    private final ConcurrentMap<String, Entry> _directory = new ConcurrentHashMap<String, Entry>();

    /**
     * @param aPeer is the peer this directory service will run on and represent
     * @param aPeerSet is the set of peers that will co-operate in providing the directory service
     */
    public Directory(Peer aPeer, PeerSet aPeerSet) {
        _peers = aPeerSet;
        _peer = aPeer;
        _dispatcher = new Dispatcher();
        _peer.add("/directory", _dispatcher);
    }

    /**
     * Ask the directory service to commence publishing of local peer details and collection of data about other peers.
     */
    public void start() {
        _peer.getTimer().schedule(new GossipTask(), 0, 30000);
    }

    /**
     * @return the attributes associated with this peer
     */
    public Map<String, String> getAttributes() {
        HashMap<String, String> myAttrs = new HashMap<String, String>();

        for (AttributeProducer ap : _producers) {
            myAttrs.putAll(ap.produce());
        }

        return myAttrs;
    }

    /**
     * @return a directory of known peers
     */
    public Map<String, Entry> getDirectory() {
        HashMap<String, Entry> myEntries = new HashMap<String, Entry>(_directory);

        myEntries.put(_peer.getAddress().toString(),
                new Entry(_peer.getAddress().toString(),
                        getAttributes(),
                        System.currentTimeMillis(),
                        _birthTime));

        return myEntries;
    }

    public void add(AttributeProducer aProducer) {
        _producers.add(aProducer);
    }

    public void add(Listener aListener) {
        _listeners.add(aListener);
    }

    private void merge(Map<String, Entry> aRemoteDirectory) {
        final List<String> myUpdatedPeers = new LinkedList<String>();

        for (Map.Entry<String, Directory.Entry> kv : aRemoteDirectory.entrySet()) {

            // Ignore my own directory
            //
            if (! kv.getKey().equals(_peer.getAddress().toString() + "/directory")) {
                boolean mySuccess = false;

                do {
                    Entry myCurrent = _directory.get(kv.getKey());

                    if (myCurrent == null) {

                        mySuccess = (_directory.putIfAbsent(kv.getKey(), kv.getValue()) == null);

                    } else if (myCurrent.getTimestamp() <= kv.getValue().getTimestamp()) {

                        mySuccess = _directory.replace(kv.getKey(), myCurrent, kv.getValue());

                    } else {
                        break;
                    }
                } while (mySuccess != true);

                if (mySuccess)
                    myUpdatedPeers.add(kv.getKey());
            }
        }

        _notifier.execute(new Runnable() {
            public void run() {
                for (Listener l : _listeners) {
                    l.updated(Directory.this, myUpdatedPeers);
                }
            }
        });
    }

    class Dispatcher implements Peer.ServiceDispatcher {
        public void dispatch(String aServicePath, HttpRequest aRequest, HttpResponse aResponse) {
            if (aRequest.getMethod().equals(HttpMethod.POST)) {

                Map<String, Entry> myDirectorySnapshot = getDirectory();
                String myJsonRemoteDir = aRequest.getContent().toString(CharsetUtil.UTF_8);

                _logger.debug("Received a directory " + myJsonRemoteDir);

                Gson myGson = new Gson();
                Type myMapType = new TypeToken<Map<String, Entry>>() {}.getType();
                Map<String, Entry> myRemoteDir = myGson.fromJson(myJsonRemoteDir, myMapType);

                _logger.debug("Unpacked: " + myRemoteDir);

                merge(myRemoteDir);

                aResponse.setContent(ChannelBuffers.copiedBuffer(myGson.toJson(myDirectorySnapshot),
                        CharsetUtil.UTF_8));
                aResponse.setStatus(HttpResponseStatus.OK);
            } else {
                aResponse.setStatus(HttpResponseStatus.BAD_REQUEST);
            }
        }
    }

    class GossipTask extends TimerTask {
        public void run() {
            final Gson myGson = new Gson();
            AsyncHttpClient myClient = _peer.getClient();

            try {
                myClient.preparePost(PeerSets.randomSelect(_peers, _peer.getAddress()).toString() +
                        "/directory").setBody(
                        myGson.toJson(getDirectory())).execute(new AsyncCompletionHandler<Response>() {

                    public Response onCompleted(Response aResponse) throws Exception {
                        Type myMapType = new TypeToken<Map<String, Entry>>() {}.getType();
                        String myResponseDir = aResponse.getResponseBody();

                        _logger.debug("Received directory " + myResponseDir);

                        try {
                            Map<String, Entry> myRemoteDir = myGson.fromJson(myResponseDir, myMapType);

                            _logger.debug("Unpacked: " + myRemoteDir);

                            merge(myRemoteDir);

                        } catch (Exception anE) {
                            _logger.error("Error in unpack", anE);
                        }

                        return aResponse;
                    }
                });
            } catch (IOException anIOE) {
                _logger.error("Received exception", anIOE);
            }
        }
    }

    public interface AttributeProducer {
        Map<String, String> produce();
    }

    public interface Listener {
        public void updated(Directory aDirectory, List<String> anUpdatedPeers);
    }

    public static class ListenerImpl implements Listener {
        private AtomicBoolean _doneFirst = new AtomicBoolean(false);
        private Map<String, Entry> _base;

        public void updated(Directory aDirectory, List<String> anUpdatedPeers) {
            if (! _doneFirst.get())
                synchronized(this) {
                    if (_doneFirst.compareAndSet(false, true))
                        _base = aDirectory.getDirectory();
                }

            // Apply update
        }
    }
}
