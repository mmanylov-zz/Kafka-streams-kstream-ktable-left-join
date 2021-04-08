package com.mmanylov.kafka_join;

import com.mmanylov.kafka_join.model.WeatherAndHotelEvent;
import com.mmanylov.kafka_join.model.WeatherEvent;
import com.mmanylov.kafka_join.model.HotelEvent;
import com.mmanylov.kafka_join.serde.Serdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.UUID;

public class InnerJoinTable {

    public static void main(String[] args) {


        new StreamRuntime("innerWindowedJoinTables" + UUID.randomUUID(), 0, 0).run((hotelTopic, weatherTopic, builder) -> {

            KTable<Long, HotelEvent> hotelStream = builder.table(Serdes.Long(), Serdes.HOTEL_SERDE, hotelTopic, "Hotels");
            KTable<Long, WeatherEvent> weatherStream = builder.table(Serdes.Long(), Serdes.WEATHER_SERDE, weatherTopic, "Weather");
            KTable<Long, HotelAndWeatherEvent> innerJoin = hotelStream.join(weatherStream, (hotel, weather) ->  new HotelAndHotelEvent(hotel, weather));
            innerJoin.print();

        });

    }
}

