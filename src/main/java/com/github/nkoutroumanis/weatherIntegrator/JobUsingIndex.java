package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.FileOutput;
import com.github.nkoutroumanis.FileParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class JobUsingIndex {

    public static void main(String args[]) {

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

            WeatherIntegrator.newWeatherIntegrator(FileParser.newFileParser("/Users/nicholaskoutroumanis/Desktop/csv/", ".csv"),
                    "/Users/nicholaskoutroumanis/Desktop/grib/", 7,
                    8, 3, "yyyy-MM-dd HH:mm:ss", stream.collect(Collectors.toList()))
                    .lruCacheMaxEntries(1).useIndex().build().integrateAndOutputToDirectory(FileOutput.newFileOutput("/Users/nicholaskoutroumanis/Desktop/myNewFolder/", true));

            Runtime rt = Runtime.getRuntime();
            System.out.println("Approximation of used Memory: " + (rt.totalMemory() - rt.freeMemory()) / 1000000 + " MB");

            long elapsedTime = (System.currentTimeMillis() - start) / 1000;
            System.out.println("Elapsed Time: " + elapsedTime + " sec");

            System.out.println("Number Of Records: " + WeatherIntegrator.numberofRecords);
            System.out.println("Number Of Hits: " + WeatherIntegrator.hits);
            System.out.println("CHR (Number Of Hits)/(Number Of Records): " + ((double) WeatherIntegrator.hits / WeatherIntegrator.numberofRecords));
            System.out.println("Throughput (records/sec): " + ((double) WeatherIntegrator.numberofRecords / elapsedTime));

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
