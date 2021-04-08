package com.mmanylov.kafka_streams_join.model;

import java.util.UUID;

public class HotelEvent {
    private long   Id;
    private String Name;
    private String Country;
    private String City;
    private String Address;
    private double Latitude;
    private double Longitude;
    private String geohash;

    @Override
    public String toString() {
        return "Hotel{" +
                "id=" + Id +
                ", geohash=" + geohash +
                '}';
    }

    public long getId() {
        return Id;
    }

    public void setId(long id) {
        this.Id = id;
    }


    public String getGeohash() {
        return geohash;
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }
}

