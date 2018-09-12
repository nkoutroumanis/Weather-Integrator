package com.github.nkoutroumanis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class JobWithoutIndex {

    public static void main(String args[]) throws InterruptedException {

        long start = System.currentTimeMillis();

        try {
            Stream<String> stream = Files.lines(Paths.get("./variables/weather-variables.txt"));

            WeatherIntegrator.newWeatherIntegrator("/Users/nicholaskoutroumanis/Desktop/csv",
                    "/Users/nicholaskoutroumanis/Desktop/folder/", "/Users/nicholaskoutroumanis/Desktop/grib_files", 3,
                    8, 7, "yyyy-MM-dd HH:mm:ss",
                    /*Arrays.asList("Temperature_isobaric")*/stream.collect(Collectors.toList()))
                    .clearExportingFiles().build().IntegrateData();

            Runtime rt = Runtime.getRuntime();
            System.out.println("Approximation of used Memory: " + (rt.totalMemory() - rt.freeMemory()) / 1000000 + " MB");
            System.out.println("Elapsed Time: "+ (System.currentTimeMillis() - start)/1000+" sec");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}