package com.github.nkoutroumanis.grib;

import com.github.nkoutroumanis.WeatherIntegrator;
import com.github.nkoutroumanis.lru.LRUCache;
import com.github.nkoutroumanis.lru.LRUCacheManager;
import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;


@State(Scope.Benchmark)
public class BenchmarksTest {

    private final WeatherIntegrator wiWithIndex = WeatherIntegrator.newWeatherIntegrator("/Users/nicholaskoutroumanis/Desktop/csv",
            "/Users/nicholaskoutroumanis/Desktop/folder/", "./grib_files", 4,
            9, 8, "dd/MM/yyyy hh:mm:ss",
            Arrays.asList("Relative_humidity_height_above_ground", "Temperature_height_above_ground"))
            .clearExportingFiles().useIndex().build();

    private final WeatherIntegrator wiWithoutIndex = WeatherIntegrator.newWeatherIntegrator("/Users/nicholaskoutroumanis/Desktop/csv",
            "/Users/nicholaskoutroumanis/Desktop/folder/", "./grib_files", 4,
            9, 8, "dd/MM/yyyy hh:mm:ss",
            Arrays.asList("Relative_humidity_height_above_ground", "Temperature_height_above_ground"))
            .clearExportingFiles().build();

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void integrateDataUsingIndex() {
        wiWithIndex.IntegrateData();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void integrateDataWithoutIndex() {
        wiWithoutIndex.IntegrateData();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public GribFilesTree gribTreeConstruction() {
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
