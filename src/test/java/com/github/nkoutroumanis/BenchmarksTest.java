package com.github.nkoutroumanis;

import com.github.nkoutroumanis.weatherIntegrator.WeatherDataObtainer;
import com.github.nkoutroumanis.weatherIntegrator.WeatherIntegrator;
import com.github.nkoutroumanis.weatherIntegrator.grib.GribFilesTree;
import com.github.nkoutroumanis.weatherIntegrator.lru.LRUCache;
import com.github.nkoutroumanis.weatherIntegrator.lru.LRUCacheManager;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
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

    private WeatherIntegrator wiWithIndex;

    {
        try {
            wiWithIndex = WeatherIntegrator.newWeatherIntegrator(filesPath, "csv",
                        gribFilesPath, 7,
                        8, 3, "yyyy-MM-dd HH:mm:ss",
                        variables)
                        .lruCacheMaxEntries(1).useIndex().build();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private WeatherIntegrator wiWithoutIndex;

    {
        try {
            wiWithoutIndex = WeatherIntegrator.newWeatherIntegrator(filesPath, "csv",
                        gribFilesPath, 7,
                        8, 3, "yyyy-MM-dd HH:mm:ss",
                        variables)
                        .lruCacheMaxEntries(1).build();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public BenchmarksTest() {
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MINUTES)
    public void integrateDataUsingIndex()  {
        try {
            wiWithIndex.integrateAndOutputToDirectory(filesExportPath);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.HOURS)
    public void integrateDataWithoutIndex()  {
        try {
            wiWithoutIndex.integrateAndOutputToDirectory(filesExportPath);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public LRUCacheManager preProcessing() {
        return LRUCacheManager.newLRUCacheManager(GribFilesTree.newGribFilesTree(gribFilesPath, gribFilesExtension),
                LRUCache.newLRUCache(4), true, Collections.unmodifiableList(variables));
    }

}
