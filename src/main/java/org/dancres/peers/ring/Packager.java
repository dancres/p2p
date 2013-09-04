package org.dancres.peers.ring;

import com.google.gson.*;
import org.dancres.peers.Directory;

import java.lang.reflect.Type;

class Packager {
    private final Gson _gson;
    private final String _ringMembershipKey;

    Packager(ConsistentHash.PositionPacker aPacker, String aRingMembershipKey) {
        _gson = new GsonBuilder().registerTypeAdapter(RingPosition.class,
                new RingPositionSerializer(aPacker)).registerTypeAdapter(RingPosition.class,
                new RingPositionDeserializer(aPacker)).create();
        _ringMembershipKey = aRingMembershipKey;
    }

    String flattenRingPositions(RingPositions aPositions) {
        return _gson.toJson(aPositions);
    }

    RingPositions extractRingPositions(Directory.Entry anEntry) {
        return _gson.fromJson(anEntry.getAttributes().get(_ringMembershipKey), RingPositions.class);
    }

    private static class RingPositionSerializer implements JsonSerializer<RingPosition> {
        private ConsistentHash.PositionPacker _positionPacker;

        RingPositionSerializer(ConsistentHash.PositionPacker aPacker) {
            _positionPacker = aPacker;
        }

        public JsonElement serialize(RingPosition ringPosition, Type type,
                                     JsonSerializationContext jsonSerializationContext) {
            JsonArray myArray = new JsonArray();

            myArray.add(new JsonPrimitive(ringPosition.getPeerName()));
            myArray.add(new JsonPrimitive(_positionPacker.pack(ringPosition.getPosition())));
            myArray.add(new JsonPrimitive(ringPosition.getBirthDate()));

            return myArray;
        }
    }

    private static class RingPositionDeserializer implements JsonDeserializer<RingPosition> {
        private ConsistentHash.PositionPacker _positionPacker;

        RingPositionDeserializer(ConsistentHash.PositionPacker aPacker) {
            _positionPacker = aPacker;
        }

        public RingPosition deserialize(JsonElement jsonElement, Type type,
                                        JsonDeserializationContext jsonDeserializationContext)
                throws JsonParseException {
            JsonArray myArray = jsonElement.getAsJsonArray();

            return new RingPosition(myArray.get(0).getAsString(), _positionPacker.unpack(myArray.get(1).getAsString()),
                    myArray.get(2).getAsLong());
        }
    }
}
