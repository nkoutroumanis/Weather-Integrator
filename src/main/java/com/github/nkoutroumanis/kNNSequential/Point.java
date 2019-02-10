package com.github.nkoutroumanis.kNNSequential;

public class Point {

    private double x;
    private double y;

    private Point(double x, double y) {
        this.x = x;
        this.y = y;
    }

    public static Point newPoint(double x, double y){
        return new Point(x, y);
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }
}
