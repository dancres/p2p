package org.dancres.concurrent;

import org.junit.Test;
import org.junit.Assert;

public class SyncdTest {
    @Test
    public void construct() {
        Syncd myS = new Syncd(5);
        Assert.assertEquals(5, myS.get());
    }

    @Test
    public void set() {
        Syncd myS = new Syncd(5);

        myS.set(25);
        Assert.assertEquals(25, myS.get());
    }

    @Test
    public void transform() {
        Syncd<Integer> myS = new Syncd(5);

        Integer myResult = myS.transform(new Syncd.Transformer<Integer>() {
            @Override
            public Integer transform(Integer aBefore) {
                return 25;
            }
        });

        Assert.assertEquals(25, myResult.intValue());
        Assert.assertEquals(25, myS.get().intValue());
    }
}
