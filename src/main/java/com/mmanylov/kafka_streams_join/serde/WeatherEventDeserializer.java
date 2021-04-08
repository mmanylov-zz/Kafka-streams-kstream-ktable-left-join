package com.mmanylov.kafka_streams_join.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mmanylov.kafka_streams_join.model.WeatherEvent;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class WeatherEventDeserializer implements Deserializer<WeatherEvent> {
    private ObjectMapper mapper = new ObjectMapper();

    public void configure(Map<String, ?> map, boolean b) {

    }

    public WeatherEvent deserialize(String s, byte[] bytes) {

        try {
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            return mapper.readValue(bytes, WeatherEvent.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {

    }
}
