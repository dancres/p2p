package org.dancres.concurrent;

import org.dancres.util.Tuple;

public class Syncd<T> {
    private T _value;

    public Syncd(T aT) {
        _value = aT;
    }

    public void set(T aValue) {
        synchronized (this) {
            _value = aValue;
        }
    }

    public T get() {
        synchronized (this) {
            return _value;
        }
    }

    public boolean testAndSet(T aBefore, T anAfter) {
        synchronized (this) {
            if (_value.equals(aBefore)) {
                _value = anAfter;
                return true;
            } else {
                return false;
            }
        }
    }

    public <U> U apply(Transformer<T, U> anFn) {
        synchronized (this) {
            Tuple<T, U> myResult = anFn.apply(_value);
            _value = myResult.getFirst();

            return myResult.getSecond();
        }
    }

    public interface Transformer<T, U> {
        public Tuple<T, U> apply(T aBefore);
    }
}
