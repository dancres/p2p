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
}
