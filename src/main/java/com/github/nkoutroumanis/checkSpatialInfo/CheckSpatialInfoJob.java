package com.github.nkoutroumanis.checkSpatialInfo;

import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.datasources.FileDatasource;

public class CheckSpatialInfoJob {
    public static void main(String args[]) throws Exception {

//        CheckSpatialDataInsideBox.newCheckSpatioTemporalInfo("/home/nikolaos/Documents/tambak",
//                2, 3).build().exportTxt("/home/nikolaos/Documents/gb");

//        CheckSpatialInfo.newCheckSpatioTemporalInfo(args[0],
//                Integer.valueOf(args[1]), Integer.valueOf(args[2])).separator(args[3]).build().exportTxt(args[4]);
        CheckSpatialInfo.newCheckSpatioTemporalInfo(FileDatasource.newFileParser("/home/nikolaos/Documents/thesis-dataset/", ".csv"),
                2, 3).separator(";").build().exportInfo(FileOutput.newFileOutput("/home/nikolaos/Desktop/infoAbout.txt", true));

    }
}
