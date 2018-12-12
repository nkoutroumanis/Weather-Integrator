package com.github.nkoutroumanis.checkSpatialInfo;

public class CheckSpatialInfoJob {
    public  static void main (String args[]){

//        CheckSpatialInfo.newCheckSpatioTemporalInfo("/home/nikolaos/Documents/tambak",
//                2, 3).build().exportTxt("/home/nikolaos/Documents/gb");

        CheckSpatialInfo.newCheckSpatioTemporalInfo(args[0],
                Integer.valueOf(args[1]), Integer.valueOf(args[2])).build().exportTxt(args[3]);


    }
}
