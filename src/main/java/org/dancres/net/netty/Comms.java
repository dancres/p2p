package org.dancres.net.netty;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelPipeline;

public class Comms {

    public interface Dispatcher {
        public byte[] dispatch(byte[] aBytes);
    }

    static ChannelPipeline stack(ChannelPipeline aPipeline, Dispatcher aDispatcher) {
        aPipeline.addLast("Unframer", new Unframer());
        aPipeline.addLast("Framer", new Framer());
        aPipeline.addLast("Handler", new Handler(aDispatcher));

        return aPipeline;
    }

    static ChannelBuffer marshall(byte[] aStructure) {
        ChannelBuffer myBuffer = ChannelBuffers.dynamicBuffer(512);

        myBuffer.writeBytes(aStructure);

        return myBuffer;
    }

    static byte[] toArray(ChannelBuffer aBuffer) {
        byte[] myBytes = new byte[aBuffer.readableBytes()];

        aBuffer.readBytes(myBytes);

        return myBytes;
    }

    static byte[] unmarshall(ChannelBuffer aBuffer) {
        return toArray(aBuffer);
    }
}
