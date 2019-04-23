package com.github.nkoutroumanis.integrator.weatherIntegrator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class JobWithoutIndex {

    public static void main(String args[]) throws InterruptedException {

        long start = System.currentTimeMillis();

        try {
            Stream<String> stream = Files.lines(Paths.get("variables/weather-variables.txt"));

            WeatherDataObtainer.newWeatherIntegrator("/home/nikolaos/Desktop/csv/",
                    "/home/nikolaos/Documents/gb-january-2018/", 7,
                    8, 3, "yyyy-MM-dd HH:mm:ss", stream.collect(Collectors.toList()))
                    .lruCacheMaxEntries(1).build().integrateData("/home/nikolaos/Desktop/eraseItt/");

            Runtime rt = Runtime.getRuntime();
            System.out.println("Approximation of used Memory: " + (rt.totalMemory() - rt.freeMemory()) / 1000000 + " MB");

            long elapsedTime = (System.currentTimeMillis() - start) / 1000;
            System.out.println("Elapsed Time: " + elapsedTime + " sec");

            System.out.println("Number Of Records: " + WeatherDataObtainer.numberofRecords);
            System.out.println("Number Of Hits: " + WeatherDataObtainer.hits);
            System.out.println("CHR (Number Of Hits)/(Number Of Records): " + ((double) WeatherDataObtainer.hits / WeatherDataObtainer.numberofRecords));
            System.out.println("Throughput (records/sec): " + ((double) WeatherDataObtainer.numberofRecords / elapsedTime));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
