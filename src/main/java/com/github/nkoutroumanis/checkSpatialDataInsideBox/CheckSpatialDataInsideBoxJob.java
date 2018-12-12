package com.github.nkoutroumanis.checkSpatialDataInsideBox;

import com.github.nkoutroumanis.histogram.Space2D;

public class CheckSpatialDataInsideBoxJob {
    public  static void main (String args[]){

//        CheckSpatialDataInsideBox.newCheckSpatioTemporalInfo("/home/nikolaos/Documents/tambak",
//                2, 3).build().exportTxt("/home/nikolaos/Documents/gb");

        CheckSpatialDataInsideBox.newCheckSpatioTemporalInfo(args[0], Integer.valueOf(args[1]), Integer.valueOf(args[2]), Space2D.newSpace2D(-31.2686628, 32.6340432, 46.6703883, 80.8218858)).build().exportTxt(args[4]);


    }
}
