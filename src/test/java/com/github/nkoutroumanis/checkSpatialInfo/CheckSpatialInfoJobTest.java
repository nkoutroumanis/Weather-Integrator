package com.github.nkoutroumanis.checkSpatialInfo;

import com.github.nkoutroumanis.datasources.Datasource;
import com.github.nkoutroumanis.datasources.FileDatasource;
import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.parsers.CsvRecordParser;
import com.github.nkoutroumanis.parsers.RecordParser;
import org.junit.Test;

public class CheckSpatialInfoJobTest {

    @Test
    public void main() throws Exception {


        Datasource ds = FileDatasource.newFileDatasource("/Users/nicholaskoutroumanis/Desktop/csv/", ".csv");
        RecordParser rp = new CsvRecordParser(ds, ";", 7, 8);
        FileOutput fileOutput = FileOutput.newFileOutput("/Users/nicholaskoutroumanis/Desktop/myNewFolder/", true);


        CheckSpatialInfo.newCheckSpatioTemporalInfo(rp).build().exportInfo(fileOutput);

    }
}