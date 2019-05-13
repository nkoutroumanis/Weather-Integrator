package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.datasources.Datasource;
import com.github.nkoutroumanis.datasources.KafkaDatasource;
import com.github.nkoutroumanis.outputs.KafkaOutput;
import com.github.nkoutroumanis.parsers.CsvRecordParser;
import com.github.nkoutroumanis.parsers.RecordParser;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JobKafkaTest {

    @Ignore
    @Test
    public void main() {

        try {
            Stream<String> stream = Files.lines(Paths.get("./src/test/resources/weather-attributes/weather-attributes.txt"));

            Datasource ds = KafkaDatasource.newKafkaDatasource("./src/test/resources/kafka/client.properties", "vfi-batch-sample", 0);

            RecordParser rp = new CsvRecordParser(ds, ";", 7, 8, 3, "yyyy-MM-dd HH:mm:ss");

            KafkaOutput kafkaOutput = KafkaOutput.newKafkaOutput("./src/test/resources/kafka/producer.properties", "nikos-trial");

            WeatherIntegrator.newWeatherIntegrator(rp,
                    "./src/test/resources/grib003Samples/", stream.collect(Collectors.toList())).filter(Rectangle.newRectangle(-180, -90, 180, 90)).removeLastValueFromRecords()
                    .lruCacheMaxEntries(1).useIndex().build().integrateAndOutputToKafkaTopic(kafkaOutput);

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}