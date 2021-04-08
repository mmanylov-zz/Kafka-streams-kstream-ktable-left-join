package com.mmanylov.kafka_streams_join.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mmanylov.kafka_streams_join.model.HotelEvent;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class HotelEventDeserializer implements Deserializer<HotelEvent>{
    private ObjectMapper mapper = new ObjectMapper();

    public void configure(Map<String, ?> map, boolean b) {

    }

    public HotelEvent deserialize(String s, byte[] bytes) {

        try {
            if(bytes == null || bytes.length == 0){
                return null;
            }
            return mapper.readValue(bytes, HotelEvent.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {

    }
}
