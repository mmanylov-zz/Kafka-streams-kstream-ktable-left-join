package com.mmanylov.kafka_streams_join;

import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Grouped;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.Properties;
import java.util.Map;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.kstream.Produced;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

// https://github.com/apache/kafka/blob/2.7/streams/examples/src/main/java/org/apache/kafka/streams/examples/pageview/PageViewTypedDemo.java
public class Join {

    // POJO classes
//    static public class HotelEvent implements JSONSerdeCompatible {
//        private String Id;
//        private String Name;
//        private String Country;
//        private String City;
//        private String Address;
//        private double Latitude;
//        private double Longitude;
//        private String geohash;
//    }
//
//    static public class WeatherEvent implements JSONSerdeCompatible {
//        private double lat;
//        private double lng;
//        private double avg_tmpr_f;
//        private double avg_tmpr_c;
//        private String wthr_date;
//        private String geohash;
//    }
//
//    static public class HotelAndWeatherEvent implements JSONSerdeCompatible {
//        private String Id;
//        private String Name;
//        private String Country;
//        private String City;
//        private String Address;
//        private String geohash;
//        private String wthr_date;
//        private double avg_tmpr_c;
//    }

    public static void main(String[] args) throws Exception {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-left-join");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, JSONSerde.class);
//        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);
//        props.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, JSONSerde.class);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        final Consumed<String, JsonNode> consumed = Consumed.with(Serdes.String(), jsonSerde);

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> weatherStream = builder.stream("weather", consumed);

        KTable<String, JsonNode> hotelStream = builder.table("hotels", consumed);

        KStream<String, JsonNode> joined = weatherStream.leftJoin(hotelStream,
                (wthr_evt, hotel_data) -> {
                    final ObjectNode jNode = JsonNodeFactory.instance.objectNode();
                    return (JsonNode) jNode
                            .put("Id", hotel_data.get("Id").textValue())
                            .put("Name", hotel_data.get("Name").textValue())
                            .put("Country", hotel_data.get("Country").textValue())
                            .put("City", hotel_data.get("City").textValue())
                            .put("Address", hotel_data.get("Address").textValue())
                            .put("geohash", hotel_data.get("geohash").textValue())
                            .put("avg_tmpr_c", wthr_evt.get("avg_tmpr_c").doubleValue())
                            .put("wthr_date", wthr_evt.get("wthr_date").textValue());

//                    final HotelAndWeatherEvent wthr_with_hotel = new HotelAndWeatherEvent();
//                    wthr_with_hotel.Id = hotel_data.Id;
//                    wthr_with_hotel.Name = hotel_data.Name;
//                    wthr_with_hotel.Country = hotel_data.Country;
//                    wthr_with_hotel.City = hotel_data.City;
//                    wthr_with_hotel.Address = hotel_data.Address;
//                    wthr_with_hotel.geohash = hotel_data.geohash;
//                    wthr_with_hotel.avg_tmpr_c = wthr_evt.avg_tmpr_c;
//                    wthr_with_hotel.wthr_date = wthr_evt.wthr_date;
//                    return wthr_with_hotel;
                }
        );//.groupByKey(Grouped.with(Serdes.String(), new JSONSerde<>()));

        joined.to("weather_and_hotels_joined", Produced.with(Serdes.String(), jsonSerde));

        final Topology topology = builder.build();
        KafkaStreams streamTableLeftJoin = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streamTableLeftJoin.close();
            }
        });

        try {
            streamTableLeftJoin.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
