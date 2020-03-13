package gr.ds.unipi.wi.lru;

import gr.ds.unipi.wi.WeatherIntegrator;
import gr.ds.unipi.wi.grib.GribFile;
import gr.ds.unipi.wi.grib.GribFileWithIndex;
import gr.ds.unipi.wi.grib.GribFileWithoutIndex;
import gr.ds.unipi.wi.grib.GribFilesTree;
import ucar.nc2.NetcdfFile;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

public final class LRUCacheManager {

    private final GribFilesTree tree;
    private final LRUCache cache;
    private final boolean useIndex;
    private final List<String> variables;
    private final int numberOfVariables;
    private final Function<String, NetcdfFile> netcdfFileFunction;

    private LRUCacheManager(GribFilesTree tree, LRUCache cache, boolean useIndex, List<String> variables, Function<String, NetcdfFile> netcdfFileFunction) {
        this.tree = tree;
        this.cache = cache;
        this.useIndex = useIndex;
        this.variables = Collections.unmodifiableList(variables);
        this.numberOfVariables = variables.size();

        this.netcdfFileFunction = netcdfFileFunction;

    }

    public static LRUCacheManager newLRUCacheManager(GribFilesTree tree, LRUCache cache, boolean useIndex, List<String> variables, Function<String, NetcdfFile> netcdfFileFunction) {
        return new LRUCacheManager(tree, cache, useIndex, variables, netcdfFileFunction);
    }

    //we can get safely the size because list is unmodifiable
    public int getNumberOfVariables() {
        return numberOfVariables;
    }

    public List<Object> getData(Date date, double lat, double lon) throws IOException {

        String choosenGribFilePath = tree.getFilePathByUnixTime(date.getTime() / 1000L);

        if (!isGribFileContainedInCache(choosenGribFilePath)) {
            if (useIndex) {
                cache.put(choosenGribFilePath, GribFileWithIndex.newGribFileWithIndex(choosenGribFilePath, variables, netcdfFileFunction));
            } else {
                cache.put(choosenGribFilePath, GribFileWithoutIndex.newGribFileWithoutIndex(choosenGribFilePath, variables, netcdfFileFunction));
            }
        } else {
            WeatherIntegrator.hits++;
        }

        GribFile gribFile = (GribFile) cache.get(choosenGribFilePath);
        return gribFile.getDataValuesByLatLon(lat, lon);

    }

    private boolean isGribFileContainedInCache(String filePath) {
        return cache.containsKey(filePath);
    }


}
