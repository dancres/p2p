package org.dancres.peers;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import org.dancres.peers.messaging.Channel;
import org.dancres.peers.messaging.Listener;
import org.dancres.peers.messaging.Message;
import org.dancres.peers.messaging.MessageFactory;
import org.dancres.util.Tuple;
import org.junit.Test;
import org.junit.Assert;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class ChannelTest {
    public static class MessageImpl implements Message {
        private String _channelId;
        private long _seq;

        public MessageImpl(String aChannelId, long aSeq) {
            _channelId = aChannelId;
            _seq = aSeq;
        }

        public String getChannelId() {
            return _channelId;
        }

        public long getSeq() {
            return _seq;
        }
    }

    public static class MessageFactoryImpl implements MessageFactory<MessageImpl> {
        public MessageImpl newMsg(String aChannelId, long aSeqNum) {
            return new MessageImpl(aChannelId, aSeqNum);
        }
    }

    @Test
    public void canBuildMessage() {
        Channel<MessageImpl> myChannel = new Channel<>("rhubarb");
        MessageFactory<MessageImpl> myFactory = new MessageFactoryImpl();

        MessageImpl myNewMsg1 = myChannel.newMessage(myFactory);
        MessageImpl myNewMsg2 = myChannel.newMessage(myFactory);

        Assert.assertEquals(myChannel.getId(), myNewMsg1.getChannelId());
        Assert.assertEquals(myChannel.getId(), myNewMsg2.getChannelId());
        Assert.assertEquals("rhubarb", myChannel.getId());
        Assert.assertNotEquals(myNewMsg1.getSeq(), myNewMsg2.getSeq());
        Assert.assertTrue(myNewMsg1.getSeq() < myNewMsg2.getSeq());
    }

    @Test
    public void basicReceive() {
        Channel<MessageImpl> myChannel = new Channel<>("rhubarb");
        MessageFactory<MessageImpl> myFactory = new MessageFactoryImpl();
        ValidatingListener myListener = new ValidatingListener();
        myChannel.add(myListener);

        for (int i = 0; i < 5; i++) {
            myChannel.add(myChannel.newMessage(myFactory));
        }

        Assert.assertEquals(5, myListener.getCount());
    }

    @Test
    public void duplicateReceive() {
        Channel<MessageImpl> myChannel = new Channel<>("rhubarb");
        MessageFactory<MessageImpl> myFactory = new MessageFactoryImpl();
        ValidatingListener myListener = new ValidatingListener();
        myChannel.add(myListener);

        for (int i = 0; i < 5; i++) {
            myChannel.add(myChannel.newMessage(myFactory));
        }

        myChannel.add(new MessageImpl("rhubarb", 3));

        Assert.assertEquals(5, myListener.getCount());
    }

    @Test
    public void outOfOrderReceive() {
        Channel<MessageImpl> myChannel = new Channel<>("rhubarb");
        MessageFactory<MessageImpl> myFactory = new MessageFactoryImpl();
        ValidatingListener myListener = new ValidatingListener();
        myChannel.add(myListener);

        MessageImpl myMsg1 = myChannel.newMessage(myFactory);
        MessageImpl myMsg2 = myChannel.newMessage(myFactory);
        MessageImpl myMsg3 = myChannel.newMessage(myFactory);

        myChannel.add(myMsg1);
        myChannel.add(myMsg3);
        myChannel.add(myMsg2);

        Assert.assertEquals(3, myListener.getCount());
    }

    @Test
    public void digestGaps() {
        Channel<MessageImpl> myChannel = new Channel<>("rhubarb");
        ValidatingListener myListener = new ValidatingListener();
        myChannel.add(myListener);

        myChannel.add(new MessageImpl("rhubarb", 2));
        myChannel.add(new MessageImpl("rhubarb", 5));
        myChannel.add(new MessageImpl("rhubarb", 7));

        List<Tuple<Long, Long>> myGaps = myChannel.digest();

        Assert.assertEquals(myGaps.get(0), new Tuple<>(2L, 7L));
        Assert.assertEquals(myGaps.get(1), new Tuple<>(0L, 1L));
        Assert.assertEquals(myGaps.get(2), new Tuple<>(3L, 4L));
        Assert.assertEquals(myGaps.get(3), new Tuple<>(6L, 6L));

        Assert.assertEquals(0, myListener.getCount());
    }

    @Test
    public void satisfyBestWeCan() {
        Channel<MessageImpl> myChannel = new Channel<>("rhubarb");
        ValidatingListener myListener = new ValidatingListener();
        myChannel.add(myListener);

        myChannel.add(new MessageImpl("rhubarb", 2));
        myChannel.add(new MessageImpl("rhubarb", 5));
        myChannel.add(new MessageImpl("rhubarb", 7));

        List<Tuple<Long, Long>> myGaps = Arrays.asList(new Tuple<>(0L, 10L), new Tuple<>(0L, 10L));

        List<MessageImpl> myMessages = myChannel.fulfil(myGaps);
        Assert.assertEquals(3, myMessages.size());

        myMessages = myChannel.fulfil(Channel.NO_DIGEST);
        Assert.assertEquals(3, myMessages.size());
    }

    @Test
    public void satisfyGaps() {
        Channel<MessageImpl> myChannel = new Channel<>("rhubarb");
        MessageFactory<MessageImpl> myFactory = new MessageFactoryImpl();
        ValidatingListener myListener = new ValidatingListener();
        myChannel.add(myListener);

        for (int i = 0; i < 100; i++) {
            myChannel.add(myChannel.newMessage(myFactory));
        }

        List<Tuple<Long, Long>> myGaps = Arrays.asList(new Tuple<>(0L, 2L), new Tuple<>(50L, 60L), new Tuple<>(98L, 99L));
        List<Tuple<Long, Long>> myExtent = Arrays.asList(new Tuple<>(0L, 99L));

        LinkedList<Long> myMembers = new LinkedList<>();
        for (Tuple<Long, Long> myGap : myGaps) {
            Range<Long> myRange = Range.closed(myGap.getFirst(), myGap.getSecond());
            ContiguousSet<Long> myMemberSubset = ContiguousSet.create(myRange, DiscreteDomain.longs());

            myMembers.addAll(myMemberSubset);
        }

        Assert.assertEquals(16, myMembers.size());

        LinkedList<Tuple<Long, Long>> myDigest = new LinkedList<>();
        myDigest.addAll(myExtent);
        myDigest.addAll(myGaps);

        List<MessageImpl> myMessages = myChannel.fulfil(myDigest);
        Assert.assertEquals(16, myMessages.size());

        for (MessageImpl myMsg : myMessages) {
            if (myMembers.contains(myMsg.getSeq())) {
                myMembers.remove(myMsg.getSeq());
            } else
                Assert.fail("Didn't find member " + myMsg.getSeq());
        }

        Assert.assertEquals(0, myMembers.size());
    }

    class ValidatingListener implements Listener<MessageImpl> {
        private long _count = 0;

        public void arrived(Iterable<MessageImpl> aMessages) {
            for (MessageImpl myMsg : aMessages) {
                if (myMsg.getSeq() != _count)
                    Assert.fail("Out of order message " + _count + " got " + myMsg.getSeq());

                _count++;
            }
        }

        public long getCount() {
            return _count;
        }
    }
}
