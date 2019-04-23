package com.github.nkoutroumanis.integrator.weatherIntegrator.lru;

import com.github.nkoutroumanis.integrator.weatherIntegrator.WeatherDataObtainer;
import com.github.nkoutroumanis.integrator.weatherIntegrator.grib.GribFile;
import com.github.nkoutroumanis.integrator.weatherIntegrator.grib.GribFileWithIndex;
import com.github.nkoutroumanis.integrator.weatherIntegrator.grib.GribFileWithoutIndex;
import com.github.nkoutroumanis.integrator.weatherIntegrator.grib.GribFilesTree;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public final class LRUCacheManager {

    private final GribFilesTree tree;
    private final LRUCache cache;
    private final boolean useIndex;
    private final List<String> variables;
    private final int numberOfVariables;

    private LRUCacheManager(GribFilesTree tree, LRUCache cache, boolean useIndex, List<String> variables) {
        this.tree = tree;
        this.cache = cache;
        this.useIndex = useIndex;
        this.variables = Collections.unmodifiableList(variables);

        this.numberOfVariables = variables.size();
    }

    //we can get safely the size because list is unmodifiable
    public int getNumberOfVariables() {
        return numberOfVariables;
    }

    public List<String> getData(Date date, double lat, double lon) throws IOException {

        String choosenGribFilePath = tree.getFilePathByUnixTime(date.getTime() / 1000L);

        if (!isGribFileContainedInCache(choosenGribFilePath)) {
            if (useIndex) {
                cache.put(choosenGribFilePath, GribFileWithIndex.newGribFileWithIndex(choosenGribFilePath, variables));
            } else {
                cache.put(choosenGribFilePath, GribFileWithoutIndex.newGribFileWithoutIndex(choosenGribFilePath, variables));
            }
        } else {
            //WeatherDataObtainer.hits++;
        }

        GribFile gribFile = (GribFile) cache.get(choosenGribFilePath);
        return gribFile.getDataValuesByLatLon(lat, lon);

    }

    private boolean isGribFileContainedInCache(String filePath) {
        return cache.containsKey(filePath);
    }

    public static LRUCacheManager newLRUCacheManager(GribFilesTree tree, LRUCache cache, boolean useIndex, List<String> variables) {
        return new LRUCacheManager(tree, cache, useIndex, variables);
    }


}
