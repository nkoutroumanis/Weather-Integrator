package com.github.nkoutroumanis.checkSpatialDataInsideBox;

import com.github.nkoutroumanis.FileOutput;
import com.github.nkoutroumanis.FileParser;
import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.histogram.Space2D;

public class CheckSpatialDataInsideBoxJob {
    public static void main(String args[]) throws Exception {

        CheckSpatialDataInsideBox.newCheckSpatioTemporalInfo(FileParser.newFileParser("/home/nikolaos/Documents/tambak",".csv"),
                2, 3, Rectangle.newRectangle(-106.7282958, -12.5515792, 98.1731682, 82.0)).build().exportInfo(FileOutput.newFileOutput("/home/nikolaos/Documents/gb", true));

    }
}
