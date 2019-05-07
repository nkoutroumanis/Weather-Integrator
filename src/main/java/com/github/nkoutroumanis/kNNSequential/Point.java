package com.github.nkoutroumanis.kNNSequential;

import com.github.nkoutroumanis.Rectangle;

public class Point {

    private double x;
    private double y;

    private Point(double x, double y) throws Exception {

        if (checkLongitude(x)) {
            throw new Exception("Longitude out of range");
        }
        if (checkLatitude(y)) {
            throw new Exception("Latitude out of range");
        }

        this.x = x;
        this.y = y;
    }

    public static Point newPoint(double x, double y) throws Exception {
        return new Point(x, y);
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    private boolean checkLongitude(double longitude) {
        return Rectangle.longitudeOutOfRange.test(longitude);
    }

    private boolean checkLatitude(double checkLatitude) {
        return Rectangle.latitudeOutOfRange.test(checkLatitude);

    }
}
