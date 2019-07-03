package com.github.nkoutroumanis.parsers.util;

public interface SpatioTemporalPoint {

    double getLongitude();
    double getLatitude();
    long getTimestamp();
    String getId();

}
