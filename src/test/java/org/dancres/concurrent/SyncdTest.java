package org.dancres.concurrent;

import org.dancres.util.Tuple;
import org.junit.Test;
import org.junit.Assert;

public class SyncdTest {
    @Test
    public void construct() {
        Syncd<Integer> myS = new Syncd<>(5);
        Assert.assertEquals(5, myS.get().intValue());
    }

    @Test
    public void set() {
        Syncd<Integer> myS = new Syncd<>(5);

        myS.set(25);
        Assert.assertEquals(25, myS.get().intValue());
    }

    @Test
    public void transform() {
        Syncd<Integer> myS = new Syncd<>(5);

        Integer myResult = myS.apply(new Syncd.Transformer<Integer, Integer>() {
            @Override
            public Tuple<Integer, Integer> apply(Integer aBefore) {
                return new Tuple<>(25, 4);
            }
        });

        Assert.assertEquals(4, myResult.intValue());
        Assert.assertEquals(25, myS.get().intValue());
    }
}
