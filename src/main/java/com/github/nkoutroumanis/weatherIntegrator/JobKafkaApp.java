package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.KafkaOutput;
import com.github.nkoutroumanis.KafkaParser;
import com.github.nkoutroumanis.Rectangle;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class JobKafkaApp {

    public static void main(String args[]) {

        /*------------------
         *
         * REMEMBER TO REMOVE THE LINE 165 FROM THE weather Integrator class. -
         * it works only for the case of having the semicolumn on the last column.
         *
         *
         * ------------------
         * */


        Config conf = ConfigFactory.parseFile(new File(args[0]));
        Config wi = conf.getConfig("wi");
        Config filter = conf.getConfig("filter");

        try {
            Stream<String> stream = Files.lines(Paths.get(wi.getString("variablesPath")));

            WeatherIntegrator.newWeatherIntegrator(KafkaParser.newKafkaParser(wi.getString("consumerPropertiesPath"), wi.getString("consumerTopic"), wi.getInt("poll")),
                    wi.getString("gribFilesFolderPath"), wi.getInt("numberOfColumnLongitude"),
                    wi.getInt("numberOfColumnLatitude"), wi.getInt("numberOfColumnDate"), wi.getString("dateFormat"), stream.collect(Collectors.toList()))
                    .lruCacheMaxEntries(wi.getInt("lruCacheMaxEntries")).useIndex().filter(Rectangle.newRectangle(filter.getDouble("minLon"), filter.getDouble("minLat"), filter.getDouble("maxLon"), filter.getDouble("maxLat"))).build().integrateAndOutputToKafkaTopic(KafkaOutput.newKafkaOutput(wi.getString("producerPropertiesPath"), wi.getString("producerTopic")));

            Runtime rt = Runtime.getRuntime();
            System.out.println("Approximation of used Memory: " + (rt.totalMemory() - rt.freeMemory()) / 1000000 + " MB");

            System.out.println("Elapsed Time: " + WeatherIntegrator.elapsedTime + " sec");

            System.out.println("Number Of Records: " + WeatherIntegrator.numberofRecords);
            System.out.println("Number Of Hits: " + WeatherIntegrator.hits);
            System.out.println("CHR (Number Of Hits)/(Number Of Records): " + ((double) WeatherIntegrator.hits / WeatherIntegrator.numberofRecords));
            System.out.println("Throughput (records/sec): " + ((double) WeatherIntegrator.numberofRecords / WeatherIntegrator.elapsedTime));
            System.out.println("Kafka buffer times: " + KafkaParser.buffer);

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
