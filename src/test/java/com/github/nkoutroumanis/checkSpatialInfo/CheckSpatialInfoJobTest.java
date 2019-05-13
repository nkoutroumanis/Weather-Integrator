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


        Datasource ds = FileDatasource.newFileDatasource("./src/test/resources/csv/", ".csv");
        RecordParser rp = new CsvRecordParser(ds, ";", 2, 3);
        FileOutput fileOutput = FileOutput.newFileOutput("./src/test/resources/checkSpatialInfoJob/", true);


        CheckSpatialInfo.newCheckSpatioTemporalInfo(rp).build().exportInfo(fileOutput);

    }
}