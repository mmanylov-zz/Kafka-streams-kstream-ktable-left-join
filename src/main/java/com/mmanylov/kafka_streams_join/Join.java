package com.mmanylov.kafka_streams_join;

import org.apache.kafka.common.serialization.Serdes;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.kstream.Produced;


// https://github.com/apache/kafka/blob/2.7/streams/examples/src/main/java/org/apache/kafka/streams/examples/pageview/PageViewTypedDemo.java
public class Join {

    public static void main(String[] args) throws Exception {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-left-join");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
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
                            .put("Id", hotel_data != null ? hotel_data.get("Id").textValue() : "UNKNOWN")
                            .put("Name", hotel_data != null ? hotel_data.get("Name").textValue() : "UNKNOWN")
                            .put("Country", hotel_data != null ? hotel_data.get("Country").textValue() : "UNKNOWN")
                            .put("City", hotel_data != null ? hotel_data.get("City").textValue() : "UNKNOWN")
                            .put("Address", hotel_data != null ? hotel_data.get("Address").textValue() : "UNKNOWN")
                            .put("geohash", wthr_evt.get("geohash").textValue())
                            .put("avg_tmpr_c", wthr_evt.get("avg_tmpr_c").doubleValue())
                            .put("wthr_date", wthr_evt.get("wthr_date").textValue());
                }
        );

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
