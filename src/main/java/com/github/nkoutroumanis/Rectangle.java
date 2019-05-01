package com.github.nkoutroumanis;

public final class Rectangle {

    //x-lon, y-lat
    private final double maxx;
    private final double minx;
    private final double maxy;
    private final double miny;

    private Rectangle(double minx, double miny, double maxx, double maxy) {
        this.minx = minx;
        this.miny = miny;
        this.maxx = maxx;
        this.maxy = maxy;
    }

    public static Rectangle newRectangle(double minx, double miny, double maxx, double maxy) {
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
}
