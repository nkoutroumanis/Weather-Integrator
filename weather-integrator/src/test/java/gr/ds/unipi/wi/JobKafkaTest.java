package gr.ds.unipi.wi;

import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.datasources.KafkaDatasource;
import gr.ds.unipi.stpin.outputs.KafkaOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.RecordParser;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JobKafkaTest {

    @Ignore("Kafka can not run while testing")
    @Test
    public void main() {

        try {
            Stream<String> stream = Files.lines(Paths.get("./src/test/resources/weather-attributes/weather-attributes.txt"));

            Datasource ds = KafkaDatasource.newKafkaDatasource("./src/test/resources/kafka/client.properties", "vfi-batch-sample", 0);

            RecordParser rp = new CsvRecordParser(ds, ";", 7, 8, 3, "yyyy-MM-dd HH:mm:ss");

            KafkaOutput kafkaOutput = KafkaOutput.newKafkaOutput("./src/test/resources/kafka/producer.properties", "nikos-trial");

            WeatherIntegrator.newWeatherIntegrator(rp,
                    "./src/test/resources/gribFiles/grib003Files/", stream.collect(Collectors.toList())).filter(Rectangle.newRectangle(-180, -90, 180, 90)).removeLastValueFromRecords()
                    .lruCacheMaxEntries(1).useIndex().build().integrate(kafkaOutput);

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}