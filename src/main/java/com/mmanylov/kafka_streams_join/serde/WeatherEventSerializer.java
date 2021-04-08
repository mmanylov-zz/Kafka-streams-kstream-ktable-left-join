package com.mmanylov.kafka_streams_join.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mmanylov.kafka_streams_join.model.WeatherEvent;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class WeatherEventSerializer implements Serializer<WeatherEvent> {
    private ObjectMapper mapper = new ObjectMapper();

    public void configure(Map<String, ?> map, boolean b) {

    }

    public byte[] serialize(String s, WeatherEvent event) {
        try {
            return mapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {

    }
}
