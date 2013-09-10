package org.dancres.peers.primitives;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.ServerSocketChannel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.util.CharsetUtil;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class HttpServer {

    public static void main(String[] anArgs) throws Exception {
        HttpServer myServer = new HttpServer(new InetSocketAddress("localhost", 8080));

        myServer.add("/test", new Handler() {
            public void process(HttpRequest aRequest, HttpResponse aResponse) {
                aResponse.setContent(ChannelBuffers.copiedBuffer("hit the test server", CharsetUtil.UTF_8));
                aResponse.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain charset=UTF-8");
            }
        });
    }

    public interface Handler {
        void process(HttpRequest aRequest, HttpResponse aResponse);
    }

    private final NioServerSocketChannelFactory _channelFactory = new NioServerSocketChannelFactory(
            Executors.newCachedThreadPool(), Executors.newCachedThreadPool());

    private final InetSocketAddress _bindAddress;
    private final URI _baseAddress;

    private final Channel _channel;
    private final Map<String, Handler> _handlers = new ConcurrentHashMap<String, Handler>();

    public HttpServer(InetSocketAddress aBindPoint) throws Exception {
        _bindAddress = aBindPoint;
        _channel = init();
        _baseAddress = new URI("http://" + _bindAddress.getHostName() + ":" + _bindAddress.getPort());
    }

    public URI getBase() throws Exception {
        return _baseAddress;
    }

    public void terminate() throws InterruptedException {
        _channel.close().await();
        _channelFactory.releaseExternalResources();
    }

    private ChannelPipeline commsStack(ChannelPipeline aPipeline) {
        aPipeline.addLast("decoder", new HttpRequestDecoder());
        aPipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
        aPipeline.addLast("encoder", new HttpResponseEncoder());
        aPipeline.addLast("deflater", new HttpContentCompressor());
        aPipeline.addLast("handler", new HttpRequestHandler());

        return aPipeline;
    }

    public void add(String aPath, Handler aHandler) {
        _handlers.put(aPath, aHandler);
    }

    private Channel init() throws InterruptedException {
        ServerSocketChannel myChannel = _channelFactory.newChannel(commsStack(Channels.pipeline()));
        myChannel.getConfig().setReuseAddress(true);
        myChannel.getConfig().setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() throws Exception {
                return commsStack(Channels.pipeline());
            }
        });

        myChannel.bind(_bindAddress).await();

        return myChannel;
    }

    class HttpRequestHandler extends SimpleChannelUpstreamHandler {
        public void messageReceived(ChannelHandlerContext aCtx, MessageEvent anE) {
            HttpRequest myRequest = (HttpRequest) anE.getMessage();
            boolean keepAlive = HttpHeaders.isKeepAlive(myRequest);
            List<String> componentPaths = paths(new QueryStringDecoder(myRequest.getUri()).getPath());
            HttpResponse myResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            boolean handled = false;

            for (String c : componentPaths) {
                if (! handled) {
                    Handler myHandler = _handlers.get(c);
                    
                    if (myHandler != null) {
                        myHandler.process(myRequest, myResponse);
                        handled = true;
                        break;
                    }
                }
            }

            // If we didn't find a handler, give a default response
            //
            if (! handled) {
                StringBuffer buf = new StringBuffer();

                buf.append("WELCOME TO THE WILD WILD WEB SERVER\r\n");
                buf.append("===================================\r\n");

                buf.append("VERSION: " + myRequest.getProtocolVersion() + "\r\n");
                buf.append("REQUEST_URI: " + myRequest.getUri() + "\r\n\r\n");

                for (String h: myRequest.getHeaderNames())
                        buf.append("HEADER: " + h + " = " + myRequest.getHeader(h) + "\r\n");

                buf.append("\r\n");

                buf.append("PATH: " + new QueryStringDecoder(myRequest.getUri()).getPath() + "\r\n");

                for (Map.Entry<String, Handler> h : _handlers.entrySet())
                        buf.append("HANDLER: " + h.getKey() + "\r\n");

                buf.append("\r\n");

                for (Map.Entry<String, List<String>> p : 
                        new QueryStringDecoder(myRequest.getUri()).getParameters().entrySet())
                    buf.append("PARAM: " + p.getKey() + " = " + p.getValue() + "\r\n");

                if (myRequest.getContent().readable()) {
                    buf.append("CONTENT: " + myRequest.getContent().toString(CharsetUtil.UTF_8) + "\r\n");
                }

                // Build the response object.
                myResponse.setContent(ChannelBuffers.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));
                myResponse.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain charset=UTF-8");
            }

            if (keepAlive) {
                // Add 'Content-Length' header only for a keep-alive connection.
                myResponse.setHeader(HttpHeaders.Names.CONTENT_LENGTH, myResponse.getContent().readableBytes());
            }

            // Encode the cookie.
            String cookieString = myRequest.getHeader(HttpHeaders.Names.COOKIE);
            if (cookieString != null) {
                Set<Cookie> cookies = (new CookieDecoder()).decode(cookieString);
                if(!cookies.isEmpty()) {
                    // Reset the cookies if necessary.
                    CookieEncoder cookieEncoder = new CookieEncoder(true);
                    for (Cookie cookie : cookies) {
                        cookieEncoder.addCookie(cookie);
                    }
                    myResponse.addHeader(HttpHeaders.Names.SET_COOKIE, cookieEncoder.encode());
                }
            }

            // Write the myResponse.
            ChannelFuture future = anE.getChannel().write(myResponse);

            // Close the non-keep-alive connection after the write operation is done.
            if (!keepAlive) {
                future.addListener(ChannelFutureListener.CLOSE);
            }
        }


        public void exceptionCaught(ChannelHandlerContext aCtx, ExceptionEvent anE) {
            anE.getCause().printStackTrace();
            anE.getChannel().close();
        }

        private List<String> paths(String aPath) {
            List<String> myList = new LinkedList<String>();
            String mySplitPath = aPath;

            do {
                myList.add(mySplitPath);
                mySplitPath = mySplitPath.substring(0, mySplitPath.lastIndexOf("/"));
            } while (mySplitPath.length() > 0);

            myList.add("/");

            return myList;
        }
    }
}
