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

    public static void main(String args[]) {

        long start = System.currentTimeMillis();

        try {
            Stream<String> stream = Files.lines(Paths.get("./src/test/resources/weather-attributes/weather-attributes.txt"));

            LRUCacheManager.newLRUCacheManager(GribFilesTree.newGribFilesTree("./src/test/resources/gribFiles/grib003Files/", ".nc"),
                    LRUCache.newLRUCache(1), true, Collections.unmodifiableList(stream.collect(Collectors.toList())));

            Runtime rt = Runtime.getRuntime();
            System.out.println("Approximation of used Memory: " + (rt.totalMemory() - rt.freeMemory()) / 1000000 + " MB");
            System.out.println("Elapsed Time: " + (System.currentTimeMillis() - start) / 1000d + " sec");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
