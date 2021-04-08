package com.mmanylov.kafka_streams_join.model;

import java.util.UUID;

public class WeatherEvent {
    private double lat;
    private double lng;
    private double avg_tmpr_f;
    private double avg_tmpr_c;
    private String wthr_date;
    private String geohash;

    @Override
    public String toString() {
        return "Weather{" +
                "wthr_date=" + wthr_date +
                ", geohash=" + geohash +
                '}';
    }

    public String getDate() {
        return wthr_date;
    }

    public void setWthr_date(String wthr_date) {
        this.wthr_date = wthr_date;
    }

    public String getGeohash() {
        return geohash;
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }
}

