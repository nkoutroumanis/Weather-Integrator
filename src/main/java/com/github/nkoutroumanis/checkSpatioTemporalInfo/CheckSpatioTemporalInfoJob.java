package com.github.nkoutroumanis.checkSpatioTemporalInfo;

import com.github.nkoutroumanis.FileOutput;
import com.github.nkoutroumanis.FileParser;

import java.io.IOException;

public class CheckSpatioTemporalInfoJob {
    public static void main(String args[]) throws Exception {

//        CheckSpatioTemporalInfo.newCheckSpatioTemporalInfo("/home/nikolaos/Documents/tambak",
//                2, 3, 4,
//                "yyyy-MM-dd HH:mm:ss").build().exportTxt("/home/nikolaos/Documents/gb");

        CheckSpatioTemporalInfo.newCheckSpatioTemporalInfo(FileParser.newFileParser("/home/nikolaos/Documents/synthetic-dataset2/", ".csv"),
                2, 3, 4,
                "yyyy-MM-dd HH:mm:ss").build().exportInfo(FileOutput.newFileOutput("/home/nikolaos/Desktop/infoAbout/",true));
    }

}
