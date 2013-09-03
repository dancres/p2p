package org.dancres.peers.primitives;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import org.junit.Assert;
import org.dancres.peers.Peer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Timer;

public class InProcessPeerTest {
    private static final String TEST_RESPONSE = "test response";

    private boolean _success = false;

    @Test
    public void test() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8081));
        AsyncHttpClient myClient = new AsyncHttpClient();

        Peer myPeer1 = new InProcessPeer(myServer, myClient, "/peer1", new Timer());

        Peer.ServiceDispatcher myDispatcher = new Dispatcher();

        myPeer1.add(new TestImpl());

        try {
            myClient.prepareGet(myPeer1.getAddress().toString() + "/test").execute(
                    new AsyncCompletionHandler<Response>() {

                        public Response onCompleted(Response aResponse) throws Exception {
                            String myBody = aResponse.getResponseBody();
                            Assert.assertNotNull(myBody);
                            Assert.assertNotSame(0, myBody.length());
                            Assert.assertEquals(myBody, TEST_RESPONSE);

                            return aResponse;
                        }
                    });
        } catch (IOException anIOE) {
        }

        Thread.sleep(1000);

        Assert.assertTrue(_success);
    }

    class TestImpl implements Peer.Service {

        public String getAddress() {
            return "/test";
        }

        public Peer.ServiceDispatcher getDispatcher() {
            return new Dispatcher();
        }
    }

    class Dispatcher implements Peer.ServiceDispatcher {

        @Override
        public void dispatch(String aServicePath, HttpRequest aRequest, HttpResponse aResponse) {
            _success = true;

            aResponse.setContent(ChannelBuffers.copiedBuffer(TEST_RESPONSE, CharsetUtil.UTF_8));
            aResponse.setStatus(HttpResponseStatus.OK);
        }
    }
}
