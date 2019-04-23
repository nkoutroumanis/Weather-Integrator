package com.github.nkoutroumanis;

import com.github.nkoutroumanis.integrator.weatherIntegrator.WeatherDataObtainer;
import com.github.nkoutroumanis.integrator.weatherIntegrator.grib.GribFilesTree;
import com.github.nkoutroumanis.integrator.weatherIntegrator.lru.LRUCache;
import com.github.nkoutroumanis.integrator.weatherIntegrator.lru.LRUCacheManager;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@State(Scope.Benchmark)
public class BenchmarksTest {

    private final static String filesPath = "/Users/nicholaskoutroumanis/Desktop/csv";
    private final static String filesExportPath = "/Users/nicholaskoutroumanis/Desktop/folder";
    private final static String gribFilesPath = "/home/nikolaos/Documents/gb-january-2018/";
    private final static String gribFilesExtension = ".grb2";


    private List<String> variables;

    {
        try {
            variables = Files.lines(Paths.get("variables/weather-variables.txt")).collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private final WeatherDataObtainer wiWithIndex = WeatherDataObtainer.newWeatherIntegrator(filesPath,
            gribFilesPath, 7,
            8, 3, "yyyy-MM-dd HH:mm:ss",
            variables)
            .lruCacheMaxEntries(1).useIndex().build();

    private final WeatherDataObtainer wiWithoutIndex = WeatherDataObtainer.newWeatherIntegrator(filesPath,
            gribFilesPath, 7,
            8, 3, "yyyy-MM-dd HH:mm:ss",
            variables)
            .lruCacheMaxEntries(1).build();

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MINUTES)
    public void integrateDataUsingIndex() {
        wiWithIndex.integrateData(filesExportPath);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.HOURS)
    public void integrateDataWithoutIndex() {
        wiWithoutIndex.integrateData(filesExportPath);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public LRUCacheManager preProcessing() {
        return LRUCacheManager.newLRUCacheManager(GribFilesTree.newGribFilesTree(gribFilesPath, gribFilesExtension),
                LRUCache.newLRUCache(4), true, Collections.unmodifiableList(variables), ";");
    }

}
