package org.dancres.net.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

class Framer extends OneToOneEncoder{
    public Object encode(ChannelHandlerContext aChannelHandlerContext, Channel aChannel, Object aBuffer) throws Exception {
        ChannelBuffer myBuffer = (ChannelBuffer) aBuffer;
        int mySize = myBuffer.writerIndex() - myBuffer.readerIndex();

        if (mySize == 0)
            return myBuffer;

        ChannelBuffer myFrame = ChannelBuffers.dynamicBuffer(512);

        myFrame.writeInt(mySize);
        myFrame.writeBytes(myBuffer);

        return myFrame;
    }
}
