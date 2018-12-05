package com.github.nkoutroumanis.histogram;

public final class Space2D {

    private final double maxx;
    private final double minx;
    private final double maxy;
    private final double miny;

    private Space2D(double minx, double miny, double maxx, double maxy) {
        this.minx = minx;
        this.miny = miny;
        this.maxx = maxx;
        this.maxy = maxy;
    }

    public static Space2D newSpace2D(double minx, double miny, double maxx, double maxy) {
        return new Space2D(minx, miny, maxx, maxy);
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
