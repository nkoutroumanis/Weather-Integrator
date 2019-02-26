package com.github.nkoutroumanis.checkSpatialInfo;

public class CheckSpatialInfoJob {
    public static void main(String args[]) {

//        CheckSpatialDataInsideBox.newCheckSpatioTemporalInfo("/home/nikolaos/Documents/tambak",
//                2, 3).build().exportTxt("/home/nikolaos/Documents/gb");

//        CheckSpatialInfo.newCheckSpatioTemporalInfo(args[0],
//                Integer.valueOf(args[1]), Integer.valueOf(args[2])).separator(args[3]).build().exportTxt(args[4]);
        CheckSpatialInfo.newCheckSpatioTemporalInfo("/home/nikolaos/Documents/thesis-dataset/",
                2, 3).separator(";").build().exportTxt("/home/nikolaos/Desktop/infoAbout.txt");

    }
}
