package com.github.nkoutroumanis.lru;

import com.github.nkoutroumanis.Job;
import com.github.nkoutroumanis.grib.GribFile;
import com.github.nkoutroumanis.grib.GribFileWithIndex;
import com.github.nkoutroumanis.grib.GribFilesTree;

import java.io.IOException;
import java.util.Date;

public final class LRUCacheManager<T> {

    private final GribFilesTree tree;
    private final LRUCache cache;

    private LRUCacheManager(GribFilesTree tree, LRUCache cache){
        this.tree = tree;
        this.cache = cache;
    }

    public String getData(Date date, float lat, float lon) throws IOException {

        String choosenGribFilePath = tree.getFilePathByUnixTime(date.getTime());

        if(Job.USE_INDEX){
            if(!isGribFileContainedInCache(choosenGribFilePath)){
                cache.put(choosenGribFilePath, GribFileWithIndex.newGribFileWithIndex(choosenGribFilePath, Job.VARIABLES_TO_BE_INTEGRATED));
            }

            GribFileWithIndex gribFileWithIndex = (GribFileWithIndex) cache.get(choosenGribFilePath);
            return gribFileWithIndex.getDataValuesByLatLon(lat, lon);
        }

        else{
            if(!isGribFileContainedInCache(choosenGribFilePath)){
                cache.put(choosenGribFilePath, GribFile.newGribFile(choosenGribFilePath, Job.VARIABLES_TO_BE_INTEGRATED));
            }

            GribFile gribFile = (GribFile) cache.get(choosenGribFilePath);
            return gribFile.getDataValuesByLatLon(lat, lon);
        }
    }

    private boolean isGribFileContainedInCache(String filePath){
        return cache.containsKey(filePath);
    }

    public static LRUCacheManager newLRUCacheManager(GribFilesTree tree, LRUCache cache){
        return new LRUCacheManager(tree, cache);
    }


}
