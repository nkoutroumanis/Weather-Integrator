package com.github.nkoutroumanis;

import java.util.function.Predicate;

public final class Rectangle {

    //x-lon, y-lat
    private final double maxx;
    private final double minx;
    private final double maxy;
    private final double miny;

    private Rectangle(double minx, double miny, double maxx, double maxy) throws Exception {

        if(checkLongitude(minx)){
            throw new Exception("Lower bound longitude out of range");
        }
        if(checkLatitude(miny)){
            throw new Exception("Lower bound latitude out of range");
        }
        if(checkLongitude(maxx)){
            throw new Exception("Upper bound longitude out of range");
        }
        if(checkLatitude(maxy)){
            throw new Exception("Upper bound latitude out of range");
        }

        this.minx = minx;
        this.miny = miny;
        this.maxx = maxx;
        this.maxy = maxy;
    }

    public static Rectangle newRectangle(double minx, double miny, double maxx, double maxy) throws Exception {
        return new Rectangle(minx, miny, maxx, maxy);
    }

    public double getMaxx() {
        return maxx;
    }

    public double getMaxy() {
        return maxy;
    }

    public double getMinx() {
        return minx;
    }

    public double getMiny() {
        return miny;
    }

    private boolean checkLongitude(double longitude){
        return longitudeOutOfRange.test(longitude);
    }

    private boolean checkLatitude(double checkLatitude){
        return latitudeOutOfRange.test(checkLatitude);

    }

    public static final Predicate<Double> longitudeOutOfRange = (longitude) -> ((Double.compare(longitude, 180) == 1) || (Double.compare(longitude, -180) == -1));
    public static final Predicate<Double> latitudeOutOfRange = (latitude) -> ((Double.compare(latitude, 90) == 1) || (Double.compare(latitude, -90) == -1));
}
