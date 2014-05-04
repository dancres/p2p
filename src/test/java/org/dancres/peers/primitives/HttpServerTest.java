package org.dancres.peers.primitives;

import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import org.dancres.net.netty.HttpServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

public class HttpServerTest {
    private boolean _success = false;

    @Test
    public void test() throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8081));

        // Test we get a basic response
        AsyncHttpClient myClient = new AsyncHttpClient();

        try {
            myClient.prepareGet(myServer.getBase().toString()).execute(new AsyncCompletionHandler<Response>() {

                public Response onCompleted(Response aResponse) throws Exception {
                    String myBody = aResponse.getResponseBody();
                    Assert.assertNotNull(myBody);
                    Assert.assertNotSame(0, myBody.length());

                    _success = true;

                    return aResponse;
                }
            });
        } catch (IOException anIOE) {
        }

        Thread.sleep(1000);

        Assert.assertTrue(_success);

        myServer.terminate();
    }
}
