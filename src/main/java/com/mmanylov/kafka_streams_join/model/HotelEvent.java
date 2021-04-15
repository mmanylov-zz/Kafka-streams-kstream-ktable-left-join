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

    public double getLatitude() {
        return Latitude;
    }

    public void setId(double Latitude) {
        this.Latitude = Latitude;
    }

    public double getLongitude() {
        return Longitude;
    }

    public void setLongitude(double Longitude) {
        this.Longitude = Longitude;
    }

    public String getName() {
        return Name;
    }

    public void setName(String Name) {
        this.Name = Name;
    }

    public String getCountry() {
        return Country;
    }

    public void setCountry(String Name) {
        this.Country = Country;
    }

    public String getCity() {
        return City;
    }

    public void setCity(String City) {
        this.City = City;
    }

    public String getAddress() {
        return Address;
    }

    public void setAddress(String Address) {
        this.Address = Address;
    }

    public String getGeohash() {
        return geohash;
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }
}

