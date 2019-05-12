package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.datasources.Datasource;
import com.github.nkoutroumanis.datasources.FileDatasource;
import com.github.nkoutroumanis.datasources.KafkaDatasource;
import com.github.nkoutroumanis.outputs.KafkaOutput;
import com.github.nkoutroumanis.parsers.CsvRecordParser;
import com.github.nkoutroumanis.parsers.RecordParser;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class JobKafkaTest {

    @Test
    public void main() {

        try {
            Stream<String> stream = Files.lines(Paths.get("./variables/weather-variables.txt"));

            Datasource ds = KafkaDatasource.newKafkaParser("./client.properties", "vfi-batch-sample", 0);

            RecordParser rp = new CsvRecordParser(ds, ";", 7, 8, 3, "yyyy-MM-dd HH:mm:ss");

            WeatherIntegrator.newWeatherIntegrator(rp,
                    "/home/wp3user01/grib-files/", stream.collect(Collectors.toList())).filter(Rectangle.newRectangle(-180, -90, 180, 90)).removeLastValueFromRecords()
                    .lruCacheMaxEntries(1).useIndex().build().integrateAndOutputToKafkaTopic("./producer.properties", "nikos-trial");

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}