package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.outputs.KafkaOutput;
import com.github.nkoutroumanis.datasources.KafkaDatasource;
import com.github.nkoutroumanis.Rectangle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class JobKafka {

    public static void main(String args[]) {

        /*------------------
         *
         * REMEMBER TO REMOVE THE LINE 165 FROM THE weather Integrator class. -
         * it works only for the case of having the semicolumn on the last column.
         *
         *
         * ------------------
         * */


        try {
            Stream<String> stream = Files.lines(Paths.get("./variables/weather-variables.txt"));

            WeatherIntegrator.newWeatherIntegrator(KafkaDatasource.newKafkaParser("./client.properties", "vfi-batch-sample", 0),
                    "/home/wp3user01/grib-files/", 7,
                    8, 3, "yyyy-MM-dd HH:mm:ss", stream.collect(Collectors.toList()))
                    .lruCacheMaxEntries(1).useIndex().filter(Rectangle.newRectangle(-10.5, 34, 37.7, 60)).build().integrateAndOutputToKafkaTopic(KafkaOutput.newKafkaOutput("./producer.properties", "nikos-trial"));

            Runtime rt = Runtime.getRuntime();
            System.out.println("Approximation of used Memory: " + (rt.totalMemory() - rt.freeMemory()) / 1000000 + " MB");

            //long elapsedTime = (System.currentTimeMillis() - start) / 1000;
            System.out.println("Elapsed Time: " + WeatherIntegrator.elapsedTime + " sec");

            System.out.println("Number Of Records: " + WeatherIntegrator.numberofRecords);
            System.out.println("Number Of Hits: " + WeatherIntegrator.hits);
            System.out.println("CHR (Number Of Hits)/(Number Of Records): " + ((double) WeatherIntegrator.hits / WeatherIntegrator.numberofRecords));
            System.out.println("Throughput (records/sec): " + ((double) WeatherIntegrator.numberofRecords / WeatherIntegrator.elapsedTime));
            System.out.println("Kafka buffer times: " + KafkaDatasource.buffer);
        } catch (IOException | ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
