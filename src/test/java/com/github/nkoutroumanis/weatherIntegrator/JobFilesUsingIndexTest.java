package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.datasources.Datasource;
import com.github.nkoutroumanis.datasources.FileDatasource;
import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.parsers.CsvRecordParser;
import com.github.nkoutroumanis.parsers.Record;
import com.github.nkoutroumanis.parsers.RecordParser;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class JobFilesUsingIndexTest {

    @Test
    public void main() {


        long start = System.currentTimeMillis();

        try {
            Stream<String> stream = Files.lines(Paths.get("./variables/weather-variables.txt"));

            Datasource ds = FileDatasource.newFileParser("/Users/nicholaskoutroumanis/Desktop/csv/", ".csv");

            RecordParser rp = new CsvRecordParser(ds, ";", 7, 8, 3, "yyyy-MM-dd HH:mm:ss");

            WeatherIntegrator.newWeatherIntegrator(rp ,
                    "/Users/nicholaskoutroumanis/Desktop/grib/", stream.collect(Collectors.toList())).checkLonLatRanges().removeLastValueFromRecords()
                    .lruCacheMaxEntries(1).useIndex().build().integrateAndOutputToDirectory("/Users/nicholaskoutroumanis/Desktop/myNewFolder/", true);
            
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}