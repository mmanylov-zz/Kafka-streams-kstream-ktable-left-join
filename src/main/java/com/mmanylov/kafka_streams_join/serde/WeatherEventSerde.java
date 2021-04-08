package com.mmanylov.kafka_streams_join.serde;

import com.mmanylov.kafka_streams_join.model.WeatherEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class WeatherEventSerde implements Serde<WeatherEvent> {
    public void configure(Map<String, ?> map, boolean b) {

    }

    public void close() {

    }

    public Serializer<WeatherEvent> serializer() {
        return new WeatherEventSerializer();
    }

    public Deserializer<WeatherEvent> deserializer() {
        return new WeatherEventDeserializer();
    }
}
