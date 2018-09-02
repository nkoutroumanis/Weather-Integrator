package com.github.nkoutroumanis.grib;

import com.github.nkoutroumanis.WeatherIntegrator;
import com.sun.tools.javac.util.List;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;


@State(Scope.Benchmark)
public class BenchmarksTest {

    private final WeatherIntegrator wi = WeatherIntegrator.newWeatherIntegrator("/Users/nicholaskoutroumanis/Desktop/csv",
            "/Users/nicholaskoutroumanis/Desktop/folder/", "./grib_files", 4,
            9, 8, "dd/MM/yyyy hh:mm:ss",
            List.of("Relative_humidity_height_above_ground", "Temperature_height_above_ground"))
            .clearExportingFiles().useIndex().build();

    @Benchmark
    @BenchmarkMode(Mode.All)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    public void integrateData(Blackhole bh) {
        wi.IntegrateData();
    }
}
