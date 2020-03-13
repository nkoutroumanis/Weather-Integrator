package gr.ds.unipi.wi;

import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.outputs.FileOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.wi.grib.GribFilesTree;
import gr.ds.unipi.wi.lru.LRUCache;
import gr.ds.unipi.wi.lru.LRUCacheManager;
import org.openjdk.jmh.annotations.*;
import ucar.nc2.NetcdfFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;


@State(Scope.Benchmark)
public class BenchmarksTest {

    private final static String filesPath = "./src/test/resources/csv";
    private final static String filesExportPath = "./src/test/resources/csv-enriched";
    private final static String gribFilesPath = "./src/test/resources/gribFiles/grib003Files/";
    private final static String gribFilesExtension = ".grb2";

    private final static Function<String, NetcdfFile> netcdfFileFunction = (path) -> {
        try {
            return NetcdfFile.open(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    };

    private List<String> variables;

    {
        try {
            variables = Files.lines(Paths.get("variables/weather-attributes.txt")).collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private WeatherIntegrator wiWithIndex;

    {
        try {

            wiWithIndex = WeatherIntegrator.newWeatherIntegrator(new CsvRecordParser(FileDatasource.newFileDatasource(filesPath, ".csv"), ";", 7, 8, 3, "yyyy-MM-dd HH:mm:ss"),
                    gribFilesPath,
                    variables)
                    .lruCacheMaxEntries(1).useIndex().build();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private WeatherIntegrator wiWithoutIndex;

    {
        try {
            wiWithIndex = WeatherIntegrator.newWeatherIntegrator(new CsvRecordParser(FileDatasource.newFileDatasource(filesPath, ".csv"), ";", 7, 8, 3, "yyyy-MM-dd HH:mm:ss"),
                    gribFilesPath,
                    variables)
                    .lruCacheMaxEntries(1).build();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public BenchmarksTest() {
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MINUTES)
    public void integrateDataUsingIndex() throws Exception {
        try {
            wiWithIndex.integrate(FileOutput.newFileOutput(filesExportPath, true));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.HOURS)
    public void integrateDataWithoutIndex() throws Exception {
        try {
            wiWithoutIndex.integrate(FileOutput.newFileOutput(filesExportPath, true));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public LRUCacheManager preProcessing() throws IOException {
        return LRUCacheManager.newLRUCacheManager(GribFilesTree.newGribFilesTree(gribFilesPath, gribFilesExtension, netcdfFileFunction),
                LRUCache.newLRUCache(4), true, Collections.unmodifiableList(variables), netcdfFileFunction);
    }

}
