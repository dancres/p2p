package org.dancres.peers;

import com.google.gson.Gson;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class GsonTest {
    @Test
    public void basic() {
        Gson myGson = new Gson();

        Map<String, String> myMap = new HashMap<String, String>();
        myMap.put("a", "b");
        myMap.put("c", "d");

        Wrapper myTest = new Wrapper(1, myMap);

        String myFlat = myGson.toJson(myTest);

        System.err.println("Result of flatten: " + myFlat);

        Wrapper myUnflattened = myGson.fromJson(myFlat, Wrapper.class);

        System.err.println("Result of unflatten: " + myUnflattened);
    }

    static class Wrapper {
        private int _version;
        private Map<String, String> _props;

        Wrapper(int aVersion, Map<String, String> aProps) {
            _version = aVersion;
            _props = aProps;
        }

        public String toString() {
            return _version + " : " + _props;
        }
    }
}
