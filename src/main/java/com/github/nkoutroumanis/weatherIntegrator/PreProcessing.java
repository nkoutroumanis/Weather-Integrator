package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.weatherIntegrator.grib.GribFilesTree;
import com.github.nkoutroumanis.weatherIntegrator.lru.LRUCache;
import com.github.nkoutroumanis.weatherIntegrator.lru.LRUCacheManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class PreProcessing {

    public static void main(String args[]) throws InterruptedException {

        long start = System.currentTimeMillis();

        try {
            Stream<String> stream = Files.lines(Paths.get("variables/weather-variables.txt"));

            LRUCacheManager.newLRUCacheManager(GribFilesTree.newGribFilesTree("/Users/nicholaskoutroumanis/Desktop/grib_files", ".grb2"),
                    LRUCache.newLRUCache(4), true, Collections.unmodifiableList(stream.collect(Collectors.toList())), ";");

            Runtime rt = Runtime.getRuntime();
            System.out.println("Approximation of used Memory: " + (rt.totalMemory() - rt.freeMemory()) / 1000000 + " MB");
            System.out.println("Elapsed Time: " + (System.currentTimeMillis() - start) / 1000 + " sec");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
