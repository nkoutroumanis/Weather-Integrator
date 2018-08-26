package com.github.nkoutroumanis.lru;

import com.github.nkoutroumanis.ParellelJob;
import com.github.nkoutroumanis.grib.GribFile;
import com.github.nkoutroumanis.grib.GribFilesTree;

import java.io.IOException;
import java.util.Date;

public final class LRUCacheManager {

    private final GribFilesTree tree;
    private final LRUCache cache;

    private LRUCacheManager(GribFilesTree tree, LRUCache cache){
        this.tree = tree;
        this.cache = cache;
    }

    public String getData(Date date, long lat, long lon) throws IOException {


        String choosenGribFilePath = tree.getFilePathByUnixTime(date.getTime());

        if(!isGribFileContainedInCache(choosenGribFilePath)){
            cache.put(choosenGribFilePath, GribFile.newGribFile(choosenGribFilePath, ParellelJob.VARIABLES_TO_BE_INTEGRATED));
        }

        GribFile gribFile = (GribFile) cache.get(choosenGribFilePath);

        return gribFile.getDataValuesByLatLon(lat, lon);
    }

    private boolean isGribFileContainedInCache(String filePath){
        return cache.containsKey(filePath);
    }

    public static LRUCacheManager newLRUCacheManager(GribFilesTree tree, LRUCache cache){
        return newLRUCacheManager(tree, cache);
    }


}
