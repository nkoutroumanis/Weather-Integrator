package com.github.nkoutroumanis.kNNOverRangeQueries;

import java.util.Map;

public class RadiusDetermination {

    private final Map<Long,Long> histogram;

    private long numberOfCellsxAxis;
    private long numberOfCellsyAxis;

    private final double x;
    private final double y;

    private final long minXc;
    private final long maxXc;

    private final long minYc;
    private final long maxYc;

    private RadiusDetermination(Map<Long, Long> histogram, long numberOfCellsxAxis, long numberOfCellsyAxis,double minx, double miny, double maxx, double maxy) {
        this.numberOfCellsxAxis = numberOfCellsxAxis;
        this.numberOfCellsyAxis = numberOfCellsyAxis;
        this.histogram = histogram;
        x = (maxx - minx) / numberOfCellsxAxis;
        y = (maxy - miny) / numberOfCellsyAxis;

        minXc = (long) (minx / x);
        maxXc = minXc + numberOfCellsxAxis - 1;

        minYc = (long) (miny / y);
        maxYc = minYc + numberOfCellsyAxis - 1;
    }

    public static RadiusDetermination newRadiusDetermination(Map<Long, Long> histogram, long numberOfCellsxAxis, long numberOfCellsyAxis,double minx, double miny, double maxx, double maxy){

        return new RadiusDetermination(histogram,  numberOfCellsxAxis,  numberOfCellsyAxis, minx,  miny,  maxx,  maxy);
    }

    private long getIdCellOfPoint(double x, double y) {

        long xc = (long) (x / this.x);
        long yc = (long) (y / this.y);

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

    private double findTheMaxCornerDistance(double x, double y, long id) {

        long xc = id % numberOfCellsxAxis;
        long yc = id / numberOfCellsxAxis;

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

    private long getNumberOfCell(long cellId) {

        if (histogram.containsKey(cellId)) {
            //System.out.println("IN CELL "+cellId +" there are "+histogram.get(cellId));
            return histogram.get(cellId);
        } else {
            return 0;
        }
    }

    public double findRadius(double x, double y, long neighboors) {

        long xc = (long) (x / this.x);
        long yc = (long) (y / this.y);

        double distance = Integer.MIN_VALUE;

        long k = 0;
        long points = 0;

        points = getNumberOfCell(xc + (yc * numberOfCellsxAxis));

        System.out.println(points);

//        System.out.println(this.y);
//        System.out.println(y);
//        System.out.println(xc);
//        System.out.println((y/this.y));
//        System.out.println(((yc)));
//        System.out.println(((yc*100000)));

        if (points >= neighboors) {
            distance = findTheMaxCornerDistance(x, y, (xc + (yc * numberOfCellsxAxis)));
        } else {
            k++;
        }

        while (k > 0) {

            if ((xc - k) >= minXc) {
                for (long i = yc - k; i <= yc + k; i++) {
                    if (i < minYc || i > maxYc) {
                        continue;
                    }
                    points = points + getNumberOfCell((xc - k) + (i * numberOfCellsxAxis));
                }
            }


            if ((xc + k) <= maxXc) {
                for (long i = yc - k; i <= yc + k; i++) {
                    if (i < minYc || i > maxYc) {
                        continue;
                    }
                    points = points + getNumberOfCell((xc + k) + (i * numberOfCellsxAxis));
                }
            }

            if ((yc - k) >= minYc) {
                for (long i = xc - k + 1; i < xc + k; i++) {
                    if (i < minXc || i > maxXc) {
                        continue;
                    }
                    points = points + getNumberOfCell(i + ((yc - k) * numberOfCellsxAxis));
                }
            }


            if ((yc + k) <= maxYc) {
                for (long i = xc - k + 1; i < xc + k; i++) {
                    if (i < minXc || i > maxXc) {
                        continue;
                    }
                    points = points + getNumberOfCell(i + ((yc + k) * numberOfCellsxAxis));
                }
            }

            if (points < neighboors) {
                //System.out.println("not enough points "+points);
                //System.out.println(k);
                k++;

            } else {

                System.out.println("Enough points " + points);

//                System.out.println("xc: " + xc);
//                System.out.println("xc-k: " + (xc - k));
//                System.out.println("xc+k: " + (xc + k));
//                System.out.println("yc: " + yc);
//                System.out.println("yc-k: " + (yc - k));
//                System.out.println("yc+k: " + (yc + k));

                long MaximumXc = xc + k;
                long MinimumXc = xc - k;

                long MaximumYc = yc + k;
                long MinimumYc = yc - k;

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
