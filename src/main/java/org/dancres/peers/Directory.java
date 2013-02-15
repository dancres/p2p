package org.dancres.peers;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.ning.http.client.*;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Directory service that lives atop a set of peers. The set of peers are used to maintain the directory via gossip.
 * There may be many more peers that are part of the directory service though not used to track/maintain it directly.
 *
 * A directory service tracks information across all known peers. The information provided is entirely user-defined
 * via attributes. Versioning of entries is done automatically and liveness tests are also supported via timestamps.
 */
public class Directory {
    private static final Logger _logger = LoggerFactory.getLogger(Directory.class);
    private final PeerSet _peers;
    private final Peer _peer;
    private final Peer.ServiceDispatcher _dispatcher;
    private final ConcurrentMap<String, String> _attributes = new ConcurrentHashMap<String, String>();
    private final AtomicLong _version = new AtomicLong(0);

    public class Entry {
        private final String _peerName;
        private final Map<String, String> _attributes;
        private final long _version;
        private final long _timestamp;

        Entry(String aName, Map<String, String> anAttrs, long aVersion, long aTimestamp) {
            _peerName = aName;
            _attributes = anAttrs;
            _version = aVersion;
            _timestamp = aTimestamp;
        }

        public String getPeerName() {
            return _peerName;
        }

        public Map<String, String> getAttributes() {
            return _attributes;
        }

        public long getVersion() {
            return _version;
        }

        public String toString() {
            return "Directory.Entry: " + _peerName + " version: " + _version + " tstamp: " + _timestamp +
                    " attributes:" + _attributes;
        }
    }

    private final AtomicReference<Map<String, Entry>> _directory =
            new AtomicReference<Map<String, Entry>>(new HashMap<String, Entry>());

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
        return Collections.unmodifiableMap(_attributes);
    }

    /**
     * Set the value of an attribute for this peer
     *
     * @param aKey is the name of the attribute
     * @param aValue is the value of the attribute
     */
    public void setAttribute(String aKey, String aValue) {
        _attributes.put(aKey, aValue);
        _version.incrementAndGet();
    }

    /**
     * @return the directory of known peers
     */
    public Map<String, Entry> getDirectory() {
        HashMap<String, Entry> myEntries = new HashMap<String, Entry>(_directory.get());

        myEntries.put(_peer.getAddress().toString(),
                new Entry(_peer.getAddress().toString(),
                        new HashMap<String, String>(_attributes),
                        _version.get(),
                        System.currentTimeMillis()));

        return Collections.unmodifiableMap(myEntries);
    }

    /**
     * @todo Do merge of directories
     */
    class Dispatcher implements Peer.ServiceDispatcher {
        public void dispatch(String aServicePath, HttpRequest aRequest, HttpResponse aResponse) {
            if (aRequest.getMethod().equals(HttpMethod.POST)) {
                String myJsonRemoteDir = aRequest.getContent().toString(CharsetUtil.UTF_8);

                _logger.debug("Received a directory " + myJsonRemoteDir);

                Gson myGson = new Gson();
                Type myMapType = new TypeToken<Map<String, Entry>>() {}.getType();
                Map<String, Entry> myRemoteDir = myGson.fromJson(myJsonRemoteDir, myMapType);

                _logger.debug("Unpacked: " + myRemoteDir);

                _logger.warn("*** Haven't implemented directory merge yet ***");

                aResponse.setContent(ChannelBuffers.copiedBuffer(myGson.toJson(getDirectory()), CharsetUtil.UTF_8));
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
}
