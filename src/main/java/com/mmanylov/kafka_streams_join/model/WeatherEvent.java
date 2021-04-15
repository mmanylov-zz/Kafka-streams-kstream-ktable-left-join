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

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLng() {
        return lng;
    }

    public void setLng(double lng) {
        this.lng = lng;
    }

    public double getAvg_tmpr_c() {
        return avg_tmpr_c;
    }

    public void setAvg_tmpr_c(double avg_tmpr_c) {
        this.avg_tmpr_c = avg_tmpr_c;
    }

    public double getAvg_tmpr_f() {
        return avg_tmpr_f;
    }

    public void setAvg_tmpr_f(double avg_tmpr_f) {
        this.avg_tmpr_f = avg_tmpr_f;
    }

    public String getWthr_date() {
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

