package com.github.nkoutroumanis.integrator.weatherIntegrator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class JobUsingIndex {

    public static void main(String args[]) throws InterruptedException {

        /*------------------
        *
        * REMEMBER TO REMOVE THE LINE 165 FROM THE weather Integrator class. -
        * it works only for the case of having the semicolumn on the last column.
        *
        *
        * ------------------
        * */


        long start = System.currentTimeMillis();

        try {
            Stream<String> stream = Files.lines(Paths.get("./variables/weather-variables.txt"));

            WeatherIntegrator.newWeatherIntegrator("/home/nikolaos/Desktop/dokimastiko/",
                    "/home/nikolaos/Documents/grib-files/", 7,
                    8, 3, "yyyy-MM-dd HH:mm:ss", stream.collect(Collectors.toList()))
                    .lruCacheMaxEntries(1).useIndex().build().integrateData("/home/nikolaos/Desktop/apotelesmata/");

            Runtime rt = Runtime.getRuntime();
            System.out.println("Approximation of used Memory: " + (rt.totalMemory() - rt.freeMemory()) / 1000000 + " MB");

            long elapsedTime = (System.currentTimeMillis() - start) / 1000;
            System.out.println("Elapsed Time: " + elapsedTime + " sec");

            System.out.println("Number Of Records: " + WeatherIntegrator.numberofRecords);
            System.out.println("Number Of Hits: " + WeatherIntegrator.hits);
            System.out.println("CHR (Number Of Hits)/(Number Of Records): " + ((double) WeatherIntegrator.hits / WeatherIntegrator.numberofRecords));
            System.out.println("Throughput (records/sec): " + ((double) WeatherIntegrator.numberofRecords / elapsedTime));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
