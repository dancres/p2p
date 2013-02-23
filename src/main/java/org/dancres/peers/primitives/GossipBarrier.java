package org.dancres.peers.primitives;

import org.dancres.peers.Directory;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Use a gossip barrier to wait for a directory to partake in a round of gossip.
 */
public class GossipBarrier implements Directory.Listener {
    private final Lock _lock = new ReentrantLock();
    private final Condition _barrier = _lock.newCondition();

    private int _gossipCount = 0;

    public void updated(Directory aDirectory, List<Directory.Entry> aNewPeers, List<Directory.Entry> anUpdatedPeers) {
        _lock.lock();

        try {
            _gossipCount++;
            _barrier.signal();
        } finally {
            _lock.unlock();
        }
    }

    public int current() {
        _lock.lock();

        try {
            return _gossipCount;
        } finally {
            _lock.unlock();
        }
    }
    public void await(int aCurrent) throws InterruptedException {
        _lock.lock();

        try {
            while (aCurrent == _gossipCount)
                _barrier.await();
        } finally {
            _lock.unlock();
        }
    }
}
