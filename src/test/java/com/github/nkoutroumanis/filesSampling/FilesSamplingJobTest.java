package com.github.nkoutroumanis.filesSampling;

import com.github.nkoutroumanis.datasources.Datasource;
import com.github.nkoutroumanis.datasources.FileDatasource;
import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.parsers.CsvRecordParser;
import com.github.nkoutroumanis.parsers.RecordParser;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.*;

public class FilesSamplingJobTest {

    @Test
    public void main() throws Exception {

        Datasource ds = FileDatasource.newFileDatasource("./src/test/resources/csv/", ".csv");
        RecordParser rp = new CsvRecordParser(ds, ";", 2, 3, 4, "yyyy-MM-dd HH:mm:ss");
        FileOutput fileOutput = FileOutput.newFileOutput("./src/test/resources/sampling/", true);

        FilesSamping.newFilesSamping(rp, 10).build().exportSamplesToFile(fileOutput);
    }

    @Test
    public void randomNumbers() {
        Random r = new Random();
        long generatedLong = 1 + (long) (Math.random() * (10 - 1));
        System.out.println(generatedLong);
    }


}