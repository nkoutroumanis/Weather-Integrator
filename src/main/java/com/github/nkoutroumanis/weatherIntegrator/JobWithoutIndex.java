package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.FileOutput;
import com.github.nkoutroumanis.FileParser;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class JobWithoutIndex {

    public static void main(String args[]) {

        long start = System.currentTimeMillis();

        try {
            Stream<String> stream = Files.lines(Paths.get("variables/weather-variables.txt"));

            WeatherIntegrator.newWeatherIntegrator(FileParser.newFileParser("/home/nikolaos/Desktop/csv/", "csv"),
                    "/home/nikolaos/Documents/gb-january-2018/", 7,
                    8, 3, "yyyy-MM-dd HH:mm:ss", stream.collect(Collectors.toList()))
                    .lruCacheMaxEntries(1).build().integrateAndOutputToDirectory(FileOutput.newFileOutput("/home/nikolaos/Desktop/eraseItt/",true));

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
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
