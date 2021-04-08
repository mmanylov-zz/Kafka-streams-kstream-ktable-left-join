package com.mmanylov.kafka_streams_join;

import com.mmanylov.kafka_streams_join.model.HotelAndWeatherEvent;
import com.mmanylov.kafka_streams_join.model.WeatherEvent;
import com.mmanylov.kafka_streams_join.model.HotelEvent;
import com.mmanylov.kafka_streams_join.serde.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.UUID;

public class LeftJoinTable {
    public static void main(String[] args) {
        new StreamRuntime("innerWindowedJoinTables" + UUID.randomUUID(), 0, 0).run((hotelTopic, weatherTopic, builder) -> {
            KStream<Long, HotelEvent> hotelStream = builder.table(Serdes.Long(), Serdes.HOTEL_SERDE, hotelTopic, "Hotels");
            KTable<Long, WeatherEvent> weatherStream = builder.table(Serdes.Long(), Serdes.WEATHER_SERDE, weatherTopic, "Weather");
            KStream<Long, HotelAndWeatherEvent> leftJoin = hotelStream.leftJoin(weatherStream, (hotel, weather) ->  new HotelAndWeatherEvent(hotel, weather));
            leftJoin.print();
        });

    }
}

