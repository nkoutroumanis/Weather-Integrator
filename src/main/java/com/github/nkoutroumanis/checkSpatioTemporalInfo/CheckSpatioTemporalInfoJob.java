package com.github.nkoutroumanis.checkSpatioTemporalInfo;

public class CheckSpatioTemporalInfoJob {
    public static void main(String args[]) {

//        CheckSpatioTemporalInfo.newCheckSpatioTemporalInfo("/home/nikolaos/Documents/tambak",
//                2, 3, 4,
//                "yyyy-MM-dd HH:mm:ss").build().exportTxt("/home/nikolaos/Documents/gb");

        CheckSpatioTemporalInfo.newCheckSpatioTemporalInfo("/home/nikolaos/Documents/synthetic-dataset2/",
                2, 3, 4,
                "yyyy-MM-dd HH:mm:ss").build().exportTxt("/home/nikolaos/Desktop/infoAbout/");
    }

}
