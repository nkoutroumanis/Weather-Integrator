package com.github.nkoutroumanis.kNNOverRangeQueries;

import java.util.Map;

public class RadiusDetermination {

    private final Map<Integer,Integer> histogram;

    private int numberOfCellsxAxis;
    private int numberOfCellsyAxis;

    private final double x;
    private final double y;

    private final int minXc;
    private final int maxXc;

    private final int minYc;
    private final int maxYc;

    private RadiusDetermination(Map<Integer, Integer> histogram, int numberOfCellsxAxis, int numberOfCellsyAxis,double minx, double miny, double maxx, double maxy) {
        this.numberOfCellsxAxis = numberOfCellsxAxis;
        this.numberOfCellsyAxis = numberOfCellsyAxis;
        this.histogram = histogram;
        x = (maxx - minx) / numberOfCellsxAxis;
        y = (maxy - miny) / numberOfCellsyAxis;

        minXc = (int) (minx / x);
        maxXc = minXc + numberOfCellsxAxis - 1;

        minYc = (int) (miny / y);
        maxYc = minYc + numberOfCellsyAxis - 1;
    }

    public static RadiusDetermination newRadiusDetermination(Map<Integer, Integer> histogram, int numberOfCellsxAxis, int numberOfCellsyAxis,double minx, double miny, double maxx, double maxy){

        return new RadiusDetermination(histogram,  numberOfCellsxAxis,  numberOfCellsyAxis, minx,  miny,  maxx,  maxy);
    }

    private int getIdCellOfPoint(double x, double y) {

        int xc = (int) (x / this.x);
        int yc = (int) (y / this.y);

        return (xc + (yc * numberOfCellsxAxis));
    }

    private double harvesine(double lon1, double lat1, double lon2, double lat2) {

        double r = 6378.1;

        double f1 = Math.toRadians(lat1);
        double f2 = Math.toRadians(lat2);

        double df = Math.toRadians(lat2 - lat1);
        double dl = Math.toRadians(lon2 - lon1);

        double a = Math.sin(df / 2) * Math.sin(df / 2) + Math.cos(f1) * Math.cos(f2) * Math.sin(dl / 2) * Math.sin(dl / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return r * c;
    }

    private double findTheMaxCornerDistance(double x, double y, int id) {

        int xc = id % numberOfCellsxAxis;
        int yc = id / numberOfCellsxAxis;

        double upperBoundx = (xc + 1) * this.x;
        double upperBoundy = (yc + 1) * this.y;

        double lowerBoundx = xc * this.x;
        double lowerBoundy = yc * this.y;

        double distance;

        double d1 = harvesine(x, y, upperBoundx, upperBoundy);
        distance = d1;

        double d2 = harvesine(x, y, lowerBoundx, lowerBoundy);

        if (Double.compare(d2, distance) == 1) {
            distance = d2;
        }

        double d3 = harvesine(x, y, upperBoundx, lowerBoundy);

        if (Double.compare(d3, distance) == 1) {
            distance = d3;
        }

        double d4 = harvesine(x, y, lowerBoundx, upperBoundy);

        if (Double.compare(d4, distance) == 1) {
            distance = d4;
        }

        return distance;

    }

    private int getNumberOfCell(int cellId) {

        if (histogram.containsKey(cellId)) {
            return histogram.get(cellId);
        } else {
            return 0;
        }
    }

    public double findRadius(double x, double y, int neighboors) {

        int xc = (int) (x / this.x);
        int yc = (int) (y / this.y);

        double distance = Integer.MIN_VALUE;

        int k = 0;
        int points = 0;

        points = getNumberOfCell(xc + (yc * numberOfCellsxAxis));

        System.out.println(this.y);
        System.out.println(y);
        System.out.println(xc);
        System.out.println((y/this.y));
        System.out.println(((yc)));
        System.out.println(((yc*100000)));

        if (points >= neighboors) {
            distance = findTheMaxCornerDistance(x, y, (xc + (yc * numberOfCellsxAxis)));
        } else {
            k++;
        }

        while (k > 0) {

            if ((xc - k) >= minXc) {
                for (int i = yc - k; i <= yc + k; i++) {
                    if (i < minYc || i > maxYc) {
                        continue;
                    }
                    points = points + getNumberOfCell((xc - k) + (i * numberOfCellsxAxis));
                }
            }


            if ((xc + k) <= maxXc) {
                for (int i = yc - k; i <= yc + k; i++) {
                    if (i < minYc || i > maxYc) {
                        continue;
                    }
                    points = points + getNumberOfCell((xc + k) + (i * numberOfCellsxAxis));
                }
            }

            if ((yc - k) >= minYc) {
                for (int i = xc - k + 1; i < xc + k; i++) {
                    if (i < minXc || i > maxXc) {
                        continue;
                    }
                    points = points + getNumberOfCell(i + ((yc - k) * numberOfCellsxAxis));
                }
            }


            if ((yc + k) <= maxYc) {
                for (int i = xc - k + 1; i < xc + k; i++) {
                    if (i < minXc || i > maxXc) {
                        continue;
                    }
                    points = points + getNumberOfCell(i + ((yc + k) * numberOfCellsxAxis));
                }
            }

            if (points < neighboors) {
                //System.out.println("not enough points "+points);
                System.out.println(k);
                k++;

            } else {

                System.out.println("Enough points " + points);

                System.out.println("xc: " + xc);
                System.out.println("xc-k: " + (xc - k));
                System.out.println("xc+k: " + (xc + k));
                System.out.println("yc: " + yc);
                System.out.println("yc-k: " + (yc - k));
                System.out.println("yc+k: " + (yc + k));

                int MaximumXc = xc + k;
                int MinimumXc = xc - k;

                int MaximumYc = yc + k;
                int MinimumYc = yc - k;

                if (xc + k > maxXc) {
                    MaximumXc = maxXc;
                }
                if (xc - k < minXc) {
                    MinimumXc = minXc;
                }
                if (yc + k > maxYc) {
                    MaximumYc = maxYc;
                }
                if (yc - k < minYc) {
                    MinimumYc = minYc;
                }

                double d1 = findTheMaxCornerDistance(x, y, (MinimumXc + (MinimumYc * numberOfCellsxAxis)));

                if (distance < d1) {
                    distance = d1;
                }

                double d2 = findTheMaxCornerDistance(x, y, (MaximumXc + (MaximumYc * numberOfCellsxAxis)));

                if (distance < d2) {
                    distance = d2;
                }

                double d3 = findTheMaxCornerDistance(x, y, (MaximumXc + (MinimumYc * numberOfCellsxAxis)));

                if (distance < d3) {
                    distance = d3;
                }

                double d4 = findTheMaxCornerDistance(x, y, (MinimumXc + (MaximumYc * numberOfCellsxAxis)));

                if (distance < d4) {
                    distance = d4;
                }

                System.out.println("final k: " + k);
                k = -1;
            }
        }
        return distance;
    }
}
