package com.mmanylov.kafka_streams_join.serde;

import com.mmanylov.kafka_streams_join.model.HotelEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class HotelEventSerde implements Serde<HotelEvent> {
    public void configure(Map<String, ?> map, boolean b) {

    }

    public void close() {

    }

    public Serializer<HotelEvent> serializer() {
        return new HotelEventSerializer();
    }

    public Deserializer<HotelEvent> deserializer() {
        return new HotelEventDeserializer();
    }
}
