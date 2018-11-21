package com.github.nkoutroumanis.histogram;

public final class Space2D {

    private final float maxx;
    private final float maxy;
    private final float minx;
    private final float miny;

    private Space2D(float minx, float miny, float maxx, float maxy) {
        this.minx = minx;
        this.miny = miny;
        this.maxx = maxx;
        this.maxy = maxy;
    }

    public static Space2D newSpace2D(float minx, float miny, float maxx, float maxy) {
        return new Space2D(minx, miny, maxx, maxy);
    }

    public float getMaxx() {
        return maxx;
    }

    public float getMaxy() {
        return maxy;
    }

    public float getMinx() {
        return minx;
    }

    public float getMiny() {
        return miny;
    }
}
