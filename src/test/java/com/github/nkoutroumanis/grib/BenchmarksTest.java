package com.github.nkoutroumanis.grib;

import com.github.nkoutroumanis.WeatherIntegrator;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
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
            .clearExportingFiles().useIndex().build();

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void integrateDataUsingIndex(Blackhole bh) {
        wiWithIndex.IntegrateData();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void integrateDataWithoutIndex(Blackhole bh) {
        wiWithoutIndex.IntegrateData();
    }
}
