package com.mmanylov.kafka_streams_join.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mmanylov.kafka_streams_join.model.HotelAndWeatherEvent;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class HotelAndWeatherEventEventSerializer implements Serializer<HotelAndWeatherEvent> {

    private ObjectMapper mapper = new ObjectMapper();

    public void configure(Map<String, ?> map, boolean b) {

    }

    public byte[] serialize(String s, HotelAndWeatherEvent event) {
        try {
            return mapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {

    }
}
