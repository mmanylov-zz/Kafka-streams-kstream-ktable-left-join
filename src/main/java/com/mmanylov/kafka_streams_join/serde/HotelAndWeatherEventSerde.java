package com.mmanylov.kafka_streams_join.serde;

import com.mmanylov.kafka_streams_join.model.HotelAndWeatherEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class HotelAndWeatherEventSerde implements Serde<HotelAndWeatherEvent> {
    public void configure(Map<String, ?> map, boolean b) {

    }

    public void close() {

    }

    public Serializer<HotelAndWeatherEvent> serializer() {
        return new HotelAndWeatherEventEventSerializer();
    }

    public Deserializer<HotelAndWeatherEvent> deserializer() {
        return new HotelAndWeatherEventDeserializer();
    }
}
