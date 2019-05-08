package com.github.nkoutroumanis.checkSpatialDataInsideBox;

import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.datasources.FileDatasource;
import com.github.nkoutroumanis.Rectangle;

public class CheckSpatialDataInsideBoxJob {
    public static void main(String args[]) throws Exception {

        CheckSpatialDataInsideBox.newCheckSpatioTemporalInfo(FileDatasource.newFileParser("/home/nikolaos/Documents/tambak", ".csv"),
                2, 3, Rectangle.newRectangle(-106.7282958, -12.5515792, 98.1731682, 82.0)).build().exportInfo(FileOutput.newFileOutput("/home/nikolaos/Documents/gb", true));

    }
}
