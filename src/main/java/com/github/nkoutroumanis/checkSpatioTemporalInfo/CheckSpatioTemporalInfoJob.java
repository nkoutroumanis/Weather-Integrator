package com.github.nkoutroumanis.checkSpatioTemporalInfo;

public class CheckSpatioTemporalInfoJob {
    public  static void main (String args[]){

//        CheckSpatioTemporalInfo.newCheckSpatioTemporalInfo("/home/nikolaos/Documents/tambak",
//                2, 3, 4,
//                "yyyy-MM-dd HH:mm:ss").build().exportTxt("/home/nikolaos/Documents/gb");

        CheckSpatioTemporalInfo.newCheckSpatioTemporalInfo(args[0],
                Integer.valueOf(args[1]), Integer.valueOf(args[2]), Integer.valueOf(args[3]),
                args[4]).build().exportTxt(args[5]);
    }

}
