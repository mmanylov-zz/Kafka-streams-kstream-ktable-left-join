package com.mmanylov.kafka_streams_join.serde;

public class MySerdes {
    public static HotelEventSerde HOTEL_SERDE = new HotelEventSerde();
    public static WeatherEventSerde WEATHER_SERDE = new WeatherEventSerde();
    public static HotelAndWeatherEventSerde HOTEL_WEATHER_SERDE = new HotelAndWeatherEventSerde();
}
