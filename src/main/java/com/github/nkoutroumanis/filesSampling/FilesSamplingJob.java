package com.github.nkoutroumanis.filesSampling;

import com.github.nkoutroumanis.datasources.Datasource;
import com.github.nkoutroumanis.datasources.FileDatasource;
import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.parsers.CsvRecordParser;
import com.github.nkoutroumanis.parsers.RecordParser;

import java.io.IOException;

public class FilesSamplingJob {
    public static void main(String args[]) throws Exception {
        Datasource ds = FileDatasource.newFileDatasource("/home/nikolaos/Documents/thesis-dataset/", ".csv");
        RecordParser rp = new CsvRecordParser(ds, ";", 2, 3, 4, "yyyy-MM-dd HH:mm:ss");
        FileOutput fileOutput = FileOutput.newFileOutput("/home/nikolaos/Documents/thesis-dataset-sampling/", true);

        FilesSamping.newFilesSamping(rp, 1000000).build().exportSamplesToFile(fileOutput);
    }
}
