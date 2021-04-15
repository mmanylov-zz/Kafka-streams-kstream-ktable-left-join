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

public class Join {

    /**
     * A serde for any class that implements {@link JSONSerdeCompatible}. Note that the classes also need to
     * be registered in the {@code @JsonSubTypes} annotation on {@link JSONSerdeCompatible}.
     *
     * @param <T> The concrete type of the class that gets de/serialized
     */
    public static class JSONSerde<T extends JSONSerdeCompatible> implements Serializer<T>, Deserializer<T>, Serde<T> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {}

        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(final String topic, final byte[] data) {
            if (data == null) {
                return null;
            }

            try {
                return (T) OBJECT_MAPPER.readValue(data, JSONSerdeCompatible.class);
            } catch (final IOException e) {
                throw new SerializationException(e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final T data) {
            if (data == null) {
                return null;
            }

            try {
                return OBJECT_MAPPER.writeValueAsBytes(data);
            } catch (final Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public void close() {}

        @Override
        public Serializer<T> serializer() {
            return this;
        }

        @Override
        public Deserializer<T> deserializer() {
            return this;
        }
    }

    /**
     * An interface for registering types that can be de/serialized with {@link JSONSerde}.
     */
    @SuppressWarnings("DefaultAnnotationParam") // being explicit for the example
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = HotelEvent.class, name = "hotel"),
            @JsonSubTypes.Type(value = WeatherEvent.class, name = "weather"),
            @JsonSubTypes.Type(value = HotelAndWeatherEvent.class, name = "hotel_and_weather"),
    })
    public interface JSONSerdeCompatible {

    }

    // POJO classes
    static public class HotelEvent implements JSONSerdeCompatible {
        private String Id;
        private String Name;
        private String Country;
        private String City;
        private String Address;
        private double Latitude;
        private double Longitude;
        private String geohash;
    }

    static public class WeatherEvent implements JSONSerdeCompatible {
        private double lat;
        private double lng;
        private double avg_tmpr_f;
        private double avg_tmpr_c;
        private String wthr_date;
        private String geohash;
    }

    static public class HotelAndWeatherEvent implements JSONSerdeCompatible {
        private String Id;
        private String Name;
        private String Country;
        private String City;
        private String Address;
        private String geohash;
        private double avg_tmpr_c;
        private String wthr_date;
    }

    public static void main(String[] args) throws Exception {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-left-join");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, JSONSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);
        props.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, JSONSerde.class);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, WeatherEvent> weatherStream = builder.stream("weather",
                Consumed.with(Serdes.String(), new JSONSerde<>()));

        KTable<String, HotelEvent> hotelStream = builder.table("hotels",
                Consumed.with(Serdes.String(), new JSONSerde<>()));

        KStream<String, HotelAndWeatherEvent> joined = weatherStream.leftJoin(hotelStream,
                (wthr_evt, hotel_data) -> {
                    final HotelAndWeatherEvent wthr_with_hotel = new HotelAndWeatherEvent();
                    wthr_with_hotel.Id = hotel_data.Id;
                    wthr_with_hotel.Name = hotel_data.Name;
                    wthr_with_hotel.Country = hotel_data.Country;
                    wthr_with_hotel.City = hotel_data.City;
                    wthr_with_hotel.Address = hotel_data.Address;
                    wthr_with_hotel.geohash = hotel_data.geohash;
                    wthr_with_hotel.avg_tmpr_c = wthr_evt.avg_tmpr_c;
                    wthr_with_hotel.wthr_date = wthr_evt.wthr_date;

                    return wthr_with_hotel;
                }
        );//.groupByKey(Grouped.with(Serdes.String(), new JSONSerde<>()));

        joined.to("weather_and_hotels_joined", Produced.with(Serdes.String(), new JSONSerde<>()));

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
