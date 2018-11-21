package com.github.nkoutroumanis.weatherIntegrator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class JobUsingIndex {

    public static float hits = 0;
    public static float numberofRows = 0;

    public static void main(String args[]) throws InterruptedException {

        long start = System.currentTimeMillis();

        try {
            Stream<String> stream = Files.lines(Paths.get("./variables/weather-variables.txt"));

            WeatherIntegrator.newWeatherIntegrator("/home/nikolaos/Documents/tambak",
                    "/home/nikolaos/Documents/gb", 2,
                    3, 4, "yyyy-MM-dd HH:mm:ss", stream.collect(Collectors.toList()))
                    .lruCacheMaxEntries(5).useIndex().build().integrateData("/home/nikolaos/Documents/eraseItt");

            Runtime rt = Runtime.getRuntime();
            System.out.println("Approximation of used Memory: " + (rt.totalMemory() - rt.freeMemory()) / 1000000 + " MB");
            System.out.println("Elapsed Time: " + (System.currentTimeMillis() - start) / 1000 + " sec");
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Number Of Hits: " + hits);
        System.out.println("Number Of Records: " + numberofRows);
        System.out.println("(Number Of Hits)/(Number Of Records): " + hits / numberofRows);


    }

}