package com.github.nkoutroumanis.checkSpatialDataInsideBox;

import com.github.nkoutroumanis.histogram.Space2D;

public class CheckSpatialDataInsideBoxJob {
    public static void main(String args[]) {

//        CheckSpatialDataInsideBox.newCheckSpatioTemporalInfo("/home/nikolaos/Documents/tambak",
//                2, 3).build().exportTxt("/home/nikolaos/Documents/gb");

        CheckSpatialDataInsideBox.newCheckSpatioTemporalInfo(args[0], Integer.valueOf(args[1]), Integer.valueOf(args[2]), Space2D.newSpace2D(-106.7282958, -12.5515792, 98.1731682, 82.0)).build().exportTxt(args[3]);


    }
}
