package org.dancres.util;

public class Tuple<T, U> {
    private T _first;
    private U _second;

    public Tuple(T aT, U aU) {
        _first = aT;
        _second = aU;
    }

    public T getFirst() {
        return _first;
    }

    public U getSecond() {
        return _second;
    }

    public int hashCode() {
        return _first.hashCode() ^ _second.hashCode();
    }

    public boolean equals(Object anObject) {
        if (anObject instanceof Tuple) {
            Tuple myOther = (Tuple) anObject;

            return ((myOther.getFirst().equals(_first)) && (myOther.getSecond().equals(_second)));
        }

        return false;
    }

    public String toString() {
        return "( " + _first.toString() + ", " + _second.toString() + " )";
    }
}
