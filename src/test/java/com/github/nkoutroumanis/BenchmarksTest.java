package com.github.nkoutroumanis;

import com.github.nkoutroumanis.WeatherIntegrator;
import com.github.nkoutroumanis.grib.GribFilesTree;
import com.github.nkoutroumanis.lru.LRUCache;
import com.github.nkoutroumanis.lru.LRUCacheManager;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@State(Scope.Benchmark)
public class BenchmarksTest  {

    private Stream<String> variables;

    {
        try {
            variables = Files.lines(Paths.get("./variables/weather-variables.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private final WeatherIntegrator wiWithIndex = WeatherIntegrator.newWeatherIntegrator("/Users/nicholaskoutroumanis/Desktop/csv",
            "/Users/nicholaskoutroumanis/Desktop/folder/", "./grib_files", 4,
            9, 8, "dd/MM/yyyy hh:mm:ss",
            variables.collect(Collectors.toList()))
            .clearExportingFiles().useIndex().build();

    private final WeatherIntegrator wiWithoutIndex = WeatherIntegrator.newWeatherIntegrator("/Users/nicholaskoutroumanis/Desktop/csv",
            "/Users/nicholaskoutroumanis/Desktop/folder/", "./grib_files", 4,
            9, 8, "dd/MM/yyyy hh:mm:ss",
            variables.collect(Collectors.toList()))
            .clearExportingFiles().build();

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void integrateDataUsingIndex() {
        wiWithIndex.IntegrateData();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.HOURS)
    public void integrateDataWithoutIndex() {
        wiWithoutIndex.IntegrateData();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public GribFilesTree gribFilesTreeConstruction() {
        return GribFilesTree.newGribFilesTree("./grib_files", ".grb2");
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public LRUCacheManager overallPreProcessing() {
        return LRUCacheManager.newLRUCacheManager(GribFilesTree.newGribFilesTree("./grib_files", ".grb2"),
                LRUCache.newLRUCache(4), true, Collections.unmodifiableList(Arrays.asList("Temperature_height_above_ground")), ";");
    }

}
