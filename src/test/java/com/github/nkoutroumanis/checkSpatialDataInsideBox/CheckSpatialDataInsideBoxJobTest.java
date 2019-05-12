package com.github.nkoutroumanis.checkSpatialDataInsideBox;

import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.datasources.Datasource;
import com.github.nkoutroumanis.datasources.FileDatasource;
import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.parsers.CsvRecordParser;
import com.github.nkoutroumanis.parsers.RecordParser;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class CheckSpatialDataInsideBoxJobTest {

    @Test
    public void main() throws Exception {

        Datasource ds = FileDatasource.newFileDatasource("/Users/nicholaskoutroumanis/Desktop/csv/", ".csv");

        RecordParser rp = new CsvRecordParser(ds, ";", 7, 8);

        FileOutput fileOutput = FileOutput.newFileOutput("/Users/nicholaskoutroumanis/Desktop/myNewFolder/", true);

        CheckSpatialDataInsideBox.newCheckSpatioTemporalInfo(rp, Rectangle.newRectangle(-106.7282958, -12.5515792, 98.1731682, 82.0)).build().exportInfo(fileOutput);

    }
}