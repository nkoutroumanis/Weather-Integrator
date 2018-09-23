package com.github.nkoutroumanis.lru;

import com.github.nkoutroumanis.JobUsingIndex;
import com.github.nkoutroumanis.grib.*;

import java.io.IOException;
import java.util.Date;
import java.util.List;

public final class LRUCacheManager {

    private final GribFilesTree tree;
    private final LRUCache cache;
    private final boolean useIndex;
    private final List<String> variables;
    private final String separator;

    private LRUCacheManager(GribFilesTree tree, LRUCache cache, boolean useIndex, List<String> variables, String separator) {
        this.tree = tree;
        this.cache = cache;
        this.useIndex = useIndex;
        this.variables = variables;
        this.separator = separator;
    }

    public String getData(Date date, float lat, float lon) throws IOException {

        String choosenGribFilePath = tree.getFilePathByUnixTime(date.getTime() / 1000L);

        if (!isGribFileContainedInCache(choosenGribFilePath)) {
            if (useIndex) {
                cache.put(choosenGribFilePath, GribFileWithIndex.newGribFileWithIndex(choosenGribFilePath, variables, separator));
            } else {
                cache.put(choosenGribFilePath, GribFileWithoutIndex.newGribFileWithoutIndex(choosenGribFilePath, variables, separator));
            }
        } else {
            JobUsingIndex.hits++;
        }

        GribFile gribFile = (GribFile) cache.get(choosenGribFilePath);
        return gribFile.getDataValuesByLatLon(lat, lon);

//        return GribFileWithoutIndex.newGribFileWithoutIndex(tree.getFilePathByUnixTime(date.getTime() / 1000L), variables, separator).getDataValuesByLatLon(lat, lon);

    }

    private boolean isGribFileContainedInCache(String filePath) {
        return cache.containsKey(filePath);
    }

    public static LRUCacheManager newLRUCacheManager(GribFilesTree tree, LRUCache cache, boolean useIndex, List<String> variables, String separator) {
        return new LRUCacheManager(tree, cache, useIndex, variables, separator);
    }


}
