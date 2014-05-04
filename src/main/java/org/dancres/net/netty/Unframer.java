package org.dancres.net.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

class Unframer extends FrameDecoder {
    public Object decode(ChannelHandlerContext aChannelHandlerContext, Channel aChannel, ChannelBuffer aChannelBuffer) throws Exception {
        if (aChannelBuffer.readableBytes() < 4)
            return null;

        aChannelBuffer.markReaderIndex();

        int myLength = aChannelBuffer.readInt();

        if (aChannelBuffer.readableBytes() < myLength) {
            aChannelBuffer.resetReaderIndex();

            return null;
        } else {
            return aChannelBuffer.readBytes(myLength);
        }
    }
}
