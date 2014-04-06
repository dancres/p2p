package org.dancres.peers.messaging;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import org.dancres.concurrent.Syncd;
import org.dancres.util.Tuple;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A primitive for ordered delivery of messages with a reconciliation mechanism for recovering from loss via
 * e.g. gossip.
 *
 * @param <T>
 */
public class Channel<T extends Message> {
    public static final List<Tuple<Long, Long>> NO_DIGEST = ImmutableList.of(new Tuple<>(0L, 0L));
    private static final int DEFAULT_MAX_HISTORY = 200;
    private final int _maxHistory;
    private final String _id;
    private final AtomicLong _nextSeq = new AtomicLong(0);
    private final Syncd<ImmutableSortedSet<T>> _history = new Syncd<>(ImmutableSortedSet.<T>of());
    private final Syncd<ImmutableSortedSet<T>> _outstanding = new Syncd<>(ImmutableSortedSet.<T>of());
    private final Syncd<Long> _floor = new Syncd<>(0L);
    private final List<Listener<T>> _listeners = new CopyOnWriteArrayList<>();

    public Channel(String anId) {
        this(anId, DEFAULT_MAX_HISTORY);
    }

    public Channel(String anId, int aMaxHistory) {
        _id = anId;
        _maxHistory = aMaxHistory;
    }

    /**
     * @param aListener is the entity that wishes to receive notification of newly available ordered messages
     */
    public void add(Listener<T> aListener) {
        _listeners.add(aListener);
    }

    /**
     * @param aListener is the entity that no longer wishes to receive messages
     * @return is <code>true</code> if the listener was found and removed
     */
    public boolean remove(Listener<T> aListener) {
        return _listeners.remove(aListener);
    }

    /**
     * @return the unique id of this channel
     */
    public String getId() {
        return _id;
    }

    /**
     * @param aFactory is the factory that this Channel uses to construct the next <code>Message</code> in the sequence
     * @return the new <code>Message</code>
     */
    public T newMessage(MessageFactory<T> aFactory) {
        return aFactory.newMsg(_id, _nextSeq.getAndIncrement());
    }

    private class MessageComparator<Z extends Message> implements Comparator<Z> {
        public int compare(Z aMsg1, Z aMsg2) {
            return new Long(aMsg1.getSeq()).compareTo(aMsg2.getSeq());
        }
    }

    /**
     * @param aMessage is the newly received <code>Message</code> to queue and deliver to <code>Listener</code>
     *                 instances.
     */
    public void add(final T aMessage) {
        _outstanding.apply(new Syncd.Transformer<ImmutableSortedSet<T>, ImmutableSortedSet<T>>() {
            public Tuple<ImmutableSortedSet<T>, ImmutableSortedSet<T>> apply(ImmutableSortedSet<T> aBefore) {
                ImmutableSortedSet<T> myNewOutstanding =
                        new ImmutableSortedSet.Builder<>(new MessageComparator<T>()).addAll(aBefore).add(aMessage).build();

                return new Tuple<>(myNewOutstanding, myNewOutstanding);
            }
        });

        push();
    }

    /**
     * Look at the outstanding messages and determine which must continue to wait and which can be passed on
     */
    private void push() {
        ImmutableSortedSet<T> myAvailable = _outstanding.apply(
                new Syncd.Transformer<ImmutableSortedSet<T>, ImmutableSortedSet<T>>() {
                    public Tuple<ImmutableSortedSet<T>, ImmutableSortedSet<T>> apply(ImmutableSortedSet<T> aBefore) {
                        final SortedSet<T> myRemaining = new TreeSet<>(new MessageComparator<T>());

                        // Identify those messages that are sequentially contiguous from _floor and can be passed on
                        //
                        ImmutableSortedSet<T> myAccessible =
                                new ImmutableSortedSet.Builder<>(new MessageComparator<T>()).addAll(
                                        Iterables.filter(aBefore, new Predicate<T>() {
                                            public boolean apply(T aMessage) {
                                                Long myCurrent = _floor.get();

                                                if (aMessage.getSeq() == myCurrent) {
                                                    // This message can be sent out to listeners so long as we're first to try
                                                    //
                                                    if (_floor.testAndSet(myCurrent, myCurrent + 1))
                                                        return true;
                                                } else
                                                    // This message must remain
                                                    //
                                                    myRemaining.add(aMessage);

                                                // Couldn't send message out or it must remain
                                                //
                                                return false;
                                            }
                                        })
                                ).build();

                        return new Tuple<>(new ImmutableSortedSet.Builder<>(
                                new MessageComparator<T>()).addAll(myRemaining).build(),
                                myAccessible);
                    }
                }
        );

        send(myAvailable);
        archive(myAvailable);
    }

    /**
     * Deliver a set of <code>Message</code> instances to registered <code>Listener</code>s
     * @param aMessages is the messages ro deliver
     */
    private void send(Iterable<T> aMessages) {
        for (Listener<T> myL : _listeners)
            myL.arrived(aMessages);
    }

    /**
     * Archive a set of <code>Message</code> instances to the history maintaining a size no greater than that
     * which was specified at construction time.
     *
     * @param aMessages the set of messages to archive
     */
    private void archive(final ImmutableSortedSet<T> aMessages) {
        _history.apply(new Syncd.Transformer<ImmutableSortedSet<T>, ImmutableSortedSet<T>>() {
            public Tuple<ImmutableSortedSet<T>, ImmutableSortedSet<T>> apply(ImmutableSortedSet<T> aBefore) {
                ImmutableSortedSet<T> myRevisedHistory;

                if ((aBefore.size() + aMessages.size() > _maxHistory)) {
                    LinkedList<T> myCompleteHistory = new LinkedList<>(aBefore);
                    myCompleteHistory.addAll(aMessages);
                    myRevisedHistory = new ImmutableSortedSet.Builder<>(new MessageComparator<T>()).addAll(
                            new TreeSet<>(myCompleteHistory.subList(myCompleteHistory.size() - _maxHistory,
                            myCompleteHistory.size()))).build();

                } else {
                    myRevisedHistory = new ImmutableSortedSet.Builder<>(
                            new MessageComparator<T>()).addAll(aBefore).addAll(aMessages).build();
                }

                return new Tuple<>(myRevisedHistory, myRevisedHistory);
            }
        });
    }

    /**
     * @return a list of an initial tuple which is the sequence number of the oldest message we've kept and the highest
     * we've got and further tuples identifying any gaps in our sequence. Each tuple contains the first sequence number
     * missing and the sequence number of a message we already have that is closest to what we require - 1.
     * If there are no gaps an empty list is returned.
     */
    public List<Tuple<Long, Long>> digest() {
        LinkedList<Tuple<Long, Long>> myAll = new LinkedList<>();
        myAll.addAll(limits());

        Iterables.addAll(myAll, gaps());
        return myAll;
    }

    /**
     * @return all the <code>Message</code> instances in our history and any outstanding we have yet to deliver
     */
    private LinkedList<T> all() {
        LinkedList<T> myAll = new LinkedList<>(_history.get());
        myAll.addAll(_outstanding.get());

        return myAll;
    }

    /**
     * @return a list of <code>Tuple</code> identifying the gaps in our currently outstanding as per the contract for
     * <code>digest</code>.
     */
    private LinkedList<Tuple<Long, Long>> gaps() {
        LinkedList<Tuple<Long, Long>> myGaps = new LinkedList<>();
        long myFloor = _floor.get();

        for (T aMessage : _outstanding.get()) {
            if (aMessage.getSeq() != myFloor) {
                // Gap between what we expect and what we have
                //
                myGaps.add(new Tuple<>(myFloor, aMessage.getSeq() - 1));
            }

            // We would hope that the next message has sequence aMessage.getSeq() + 1
            //
            myFloor = aMessage.getSeq() + 1;
        }

        return myGaps;
    }

    /**
     * @return a single member list containing a tuple containing the extent (lowest sequence number in history
     * and the highest in outstanding) of this <code>Channel</code>'s knowledge.
     */
    private List<Tuple<Long, Long>> limits() {
        LinkedList<T> myAll = all();

        if (myAll.size() < 2)
            return NO_DIGEST;
        else
            return Arrays.asList(new Tuple<>(myAll.getFirst().getSeq(), myAll.getLast().getSeq()));
    }

    /**
     * Return all messages we have that can help satisfy the specified gaps. We do not guarantee to fill a gap
     * wholly just to contribute what we have.
     *
     * @param aGapsList the list of gaps with format ((known_range_min, known_range_max), (gap_min, gap_max),
     *                  (gap_min, gap_max).....) or NO_DIGEST (unknown range and no known gaps) causes all
     *                  messages to be returned.
     * @return the messages we can contribute
     */
    public List<T> fulfil(List<Tuple<Long, Long>> aGapsList) {
        LinkedList<T> myAll = all();
        LinkedList<Tuple<Long, Long>> myOthersGaps = new LinkedList<>(aGapsList);
        Tuple<Long, Long> myRange = limits().get(0);
        Tuple<Long, Long> myOthersRange = myOthersGaps.removeFirst();

        // If the range the other knows about ends lower than ours, there's a gap it cannot deduce which is all those
        // messages above it's max range and below ours.
        //
        if (myOthersRange.getSecond() < myRange.getSecond()) {
            myOthersGaps.add(new Tuple<>(myOthersRange.getSecond(), myRange.getSecond()));
        }

        LinkedList<T> myResults = new LinkedList<>();

        for (Tuple<Long, Long> myGap : myOthersGaps) {
            // {x | a <= x < b} is closedOpen()
            //
            final Range mySearchRange = Range.closedOpen(myGap.getFirst(), myGap.getSecond() + 1);
            Iterables.addAll(myResults, Iterables.filter(myAll,
                    new Predicate<T>() {
                        public boolean apply(T aMessage) {
                            return mySearchRange.contains(aMessage.getSeq());
                        }
                    }
            ));
        }

        return myResults;
    }
}
