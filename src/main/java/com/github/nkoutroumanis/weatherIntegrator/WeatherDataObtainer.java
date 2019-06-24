package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.weatherIntegrator.grib.GribFilesTree;
import com.github.nkoutroumanis.weatherIntegrator.lru.LRUCache;
import com.github.nkoutroumanis.weatherIntegrator.lru.LRUCacheManager;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public final class WeatherDataObtainer {

    private final LRUCacheManager lruCacheManager;

    private WeatherDataObtainer(String gribFilesFolderPath, String gribFilesExtension, int lruCacheMaxEntries, boolean useIndex, List<String> variables) {
        lruCacheManager = LRUCacheManager.newLRUCacheManager(GribFilesTree.newGribFilesTree(gribFilesFolderPath, gribFilesExtension),
                LRUCache.newLRUCache(lruCacheMaxEntries), useIndex, variables);
    }

    public static WeatherDataObtainer newWeatherDataObtainer(String gribFilesFolderPath, String gribFilesExtension, int lruCacheMaxEntries, boolean useIndex, List<String> variables) {
        return new WeatherDataObtainer(gribFilesFolderPath, gribFilesExtension, lruCacheMaxEntries, useIndex, variables);
    }

    public List<Object> obtainAttributes(double longitude, double latitude, Date date) throws IOException {
        return lruCacheManager.getData(date, latitude, longitude);
    }

}
