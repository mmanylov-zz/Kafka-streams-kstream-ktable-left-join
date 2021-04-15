package com.mmanylov.kafka_streams_join.model;

public class HotelAndWeatherEvent {

    private HotelEvent hotelEvent;
    private WeatherEvent weatherEvent;

    public HotelAndWeatherEvent(WeatherEvent weatherEvent, HotelEvent hotelEvent) {
        this.hotelEvent = hotelEvent;
        this.weatherEvent = weatherEvent;
    }

    public HotelAndWeatherEvent(){}

    public HotelEvent getHotelEvent() {
        return hotelEvent;
    }

    public void setHotelEvent(HotelEvent hotelEvent) {
        this.hotelEvent = hotelEvent;
    }

    public WeatherEvent getWeatherEvent() {
        return weatherEvent;
    }

    public void setWeatherEvent(WeatherEvent weatherEvent) {
        this.weatherEvent = weatherEvent;
    }

    @Override
    public String toString() {
        return "HotelAndWeatherEvent{" +
                "hotelEvent=" + hotelEvent +
                ", weatherEvent=" + weatherEvent +
                '}';
    }
}