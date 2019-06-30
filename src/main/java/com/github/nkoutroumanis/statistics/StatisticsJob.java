package com.github.nkoutroumanis.statistics;

import com.github.nkoutroumanis.datasources.Datasource;
import com.github.nkoutroumanis.datasources.FileDatasource;
import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.parsers.CsvRecordParser;
import com.github.nkoutroumanis.parsers.RecordParser;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;

public final class StatisticsJob {


    public static void main(String args[]) throws IOException, ParseException {

        CsvRecordParser csvRecordParser = new CsvRecordParser(FileDatasource.newFileDatasource("/home/nikolaos/Documents/zelitron-integrated-all",".csv"), "separator", 7,8,3,"yyyy-MM-dd HH:mm:ss");

        Statistics.newStatistics().build().calculateElementsFromCSVformat(csvRecordParser, Arrays.asList(26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38), FileOutput.newFileOutput("/home/nikolaos/Documents/",true));

    }

}
