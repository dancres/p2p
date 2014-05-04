package org.dancres.net.netty;

import static org.dancres.net.netty.Comms.*;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.DatagramChannel;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.oio.OioDatagramChannelFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class MulticastServer {
    private final InetAddress _localAddr;
    private final InetAddress _localIntf;
    private final InetSocketAddress _destAddr;
    private final DatagramChannelFactory _channelFactory = new OioDatagramChannelFactory(Executors.newCachedThreadPool());
    private final Channel _channel;

    public MulticastServer(String anAddr, int aPort, String anInterface, Dispatcher aDispatcher) throws Exception {
        _localAddr = InetAddress.getByName(anAddr);
        _localIntf = InetAddress.getByName(anInterface);
        _destAddr = new InetSocketAddress(_localAddr, aPort);
        _channel = init(aDispatcher, aPort);
    }

    private Channel init(Dispatcher aDispatcher, int aPort) throws Exception {
        DatagramChannel myChannel = _channelFactory.newChannel(stack(Channels.pipeline(), aDispatcher));
        myChannel.getConfig().setReuseAddress(true);
        // myChannel.getConfig().setInterface(_localIntf);
        myChannel.bind(new InetSocketAddress(aPort));
        myChannel.joinGroup(_localAddr);

        return myChannel;
    }

    public void stop() throws Exception {
        _channel.close().await();
        _channel.unbind().await();
        _channelFactory.releaseExternalResources();
    }

    public void transmit(byte[] aMsg) {
        _channel.write(marshall(aMsg), _destAddr);
    }
}
