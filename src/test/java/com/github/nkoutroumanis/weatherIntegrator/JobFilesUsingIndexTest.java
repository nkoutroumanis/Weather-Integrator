package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.datasources.Datasource;
import com.github.nkoutroumanis.datasources.FileDatasource;
import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.parsers.CsvRecordParser;
import com.github.nkoutroumanis.parsers.RecordParser;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JobFilesUsingIndexTest {

    @Test
    public void main() throws Exception {

        Stream<String> stream = Files.lines(Paths.get("./variables/weather-variables.txt"));
        Datasource ds = FileDatasource.newFileDatasource("/Users/nicholaskoutroumanis/Desktop/csv/", ".csv");
        RecordParser rp = new CsvRecordParser(ds, ";", 7, 8, 3, "yyyy-MM-dd HH:mm:ss");
        FileOutput fileOutput = FileOutput.newFileOutput("/Users/nicholaskoutroumanis/Desktop/myNewFolder/", true);

        WeatherIntegrator.newWeatherIntegrator(rp,
                "/Users/nicholaskoutroumanis/Desktop/grib/", stream.collect(Collectors.toList())).filter(Rectangle.newRectangle(-180, -90, 180, 90)).removeLastValueFromRecords()
                .lruCacheMaxEntries(1).useIndex().build().integrateAndOutputToDirectory(fileOutput);


    }
}