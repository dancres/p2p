package org.dancres.concurrent;

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

    public T transform(Transformer<T> anFn) {
        synchronized (this) {
            _value = anFn.transform(_value);

            return _value;
        }
    }

    public interface Transformer<T> {
        public T transform(T aBefore);
    }
}
