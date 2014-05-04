package org.dancres.net.netty;

import static org.dancres.net.netty.Comms.*;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Handler extends SimpleChannelHandler {
    private static final Logger _logger = LoggerFactory.getLogger(Handler.class);

    private final Dispatcher _dispatcher;

    Handler(Dispatcher aDispatcher) {
        _dispatcher = aDispatcher;
    }

    public void channelConnected(ChannelHandlerContext aContext, ChannelStateEvent anEvent) {
        _logger.info("Connected: " + anEvent.getChannel());
    }

    public void channelDisconnected(ChannelHandlerContext aContext, ChannelStateEvent anEvent) {
        _logger.info("Disconnected: " + anEvent.getChannel());
    }

    public void exceptionCaught(ChannelHandlerContext aContext, ExceptionEvent anEvent) {
        _logger.error("!!! Exception !!!", anEvent.getCause());

        anEvent.getChannel().close();
    }

    public void messageReceived(ChannelHandlerContext aContext, MessageEvent anEvent) {
        ChannelBuffer myBuffer = (ChannelBuffer) anEvent.getMessage();
        Channel myChannel = anEvent.getChannel();

        try {
            byte[] myResponse = _dispatcher.dispatch(unmarshall(myBuffer));

            if (myResponse != null)
                myChannel.write(marshall(myResponse));
        } catch (Throwable aT) {
            _logger.error("Dispatch failure", aT);
        }
    }
}
