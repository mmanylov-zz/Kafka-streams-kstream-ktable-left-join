package com.mmanylov.kafka_streams_join;

import com.mmanylov.kafka_streams_join.serde.MySerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.mmanylov.kafka_streams_join.model.WeatherEvent;
import com.mmanylov.kafka_streams_join.model.HotelEvent;
import com.mmanylov.kafka_streams_join.model.HotelAndWeatherEvent;
import org.apache.kafka.streams.kstream.Produced;

public class Join {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-left-join");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, WeatherEvent> weatherStream = builder.stream("weather",
                Consumed.with(Serdes.String(), MySerdes.WEATHER_SERDE));

        KTable<String, HotelEvent> hotelStream = builder.table("hotels",
                Consumed.with(Serdes.String(), MySerdes.HOTEL_SERDE));

        KStream<String, HotelAndWeatherEvent> joined = weatherStream.leftJoin(hotelStream,
                (leftValue, rightValue) -> new HotelAndWeatherEvent(leftValue, rightValue)
        );

        joined.to("weather_and_hotels_joined", Produced.with(Serdes.String(), MySerdes.HOTEL_WEATHER_SERDE));

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
