package gr.ds.unipi.wi;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.datasources.Datasource;
import gr.ds.unipi.stpin.datasources.KafkaDatasource;
import gr.ds.unipi.stpin.outputs.KafkaOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.RecordParser;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class JobKafka {

    public static void main(String args[]) {

        Config conf = ConfigFactory.parseFile(new File(args[0]));
        Config wi = conf.getConfig("wi");
        Config filter = conf.getConfig("filter");

        try {
            Stream<String> stream = Files.lines(Paths.get(wi.getString("variablesPath")));

            Datasource ds = KafkaDatasource.newKafkaDatasource(wi.getString("consumerPropertiesPath"), wi.getString("consumerTopic"), wi.getInt("poll"));

            RecordParser rp = new CsvRecordParser(ds, ";", wi.getInt("numberOfColumnLongitude"), wi.getInt("numberOfColumnLatitude"), wi.getInt("numberOfColumnDate"), wi.getString("dateFormat"));

            KafkaOutput kafkaOutput = KafkaOutput.newKafkaOutput(wi.getString("producerPropertiesPath"), wi.getString("producerTopic"));

            WeatherIntegrator.Builder w = WeatherIntegrator.newWeatherIntegrator(rp,
                    wi.getString("gribFilesFolderPath"), stream.collect(Collectors.toList()));

            if (wi.getBoolean("filter")) {
                w.filter(Rectangle.newRectangle(filter.getDouble("minLon"), filter.getDouble("minLat"), filter.getDouble("maxLon"), filter.getDouble("maxLat")));
            }

            if (wi.getBoolean("removeLastValueFromRecords")) {
                w.removeLastValueFromRecords();
            }

            w.lruCacheMaxEntries(wi.getInt("lruCacheMaxEntries")).gribFilesExtension(wi.getString("gribFilesExtension")).useIndex().build().integrate(kafkaOutput);


        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
