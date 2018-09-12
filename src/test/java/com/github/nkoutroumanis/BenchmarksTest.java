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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@State(Scope.Benchmark)
public class BenchmarksTest  {

    private final static String filesPath="/Users/nicholaskoutroumanis/Desktop/csv";
    private final static String filesExportPath="/Users/nicholaskoutroumanis/Desktop/folder/";
    private final static String gribFilesPath="/Users/nicholaskoutroumanis/Desktop/grib_files";
    private final static String gribFilesExtension=".grb2";



    private List<String> variables;

    {
        try {
            variables = Files.lines(Paths.get("./variables/weather-variables.txt")).collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private final WeatherIntegrator wiWithIndex = WeatherIntegrator.newWeatherIntegrator(filesPath,
            filesExportPath, gribFilesPath, 3,
            8, 7, "yyyy-MM-dd HH:mm:ss",
            variables)
            .clearExportingFiles().useIndex().build();

    private final WeatherIntegrator wiWithoutIndex = WeatherIntegrator.newWeatherIntegrator(filesPath,
            filesExportPath, gribFilesPath, 3,
            8, 7, "yyyy-MM-dd HH:mm:ss",
            variables)
            .clearExportingFiles().build();

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MINUTES)
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
    public LRUCacheManager preProcessing() {
        return LRUCacheManager.newLRUCacheManager(GribFilesTree.newGribFilesTree(gribFilesPath, gribFilesExtension),
                LRUCache.newLRUCache(4), true, Collections.unmodifiableList(variables), ";");
    }

}
