package com.github.nkoutroumanis;

public final class LRUCacheManager {

    private final GribFilesTree tree;
    private final LRUCache cache;

    private LRUCacheManager(GribFilesTree tree, LRUCache cache){
        this.tree = tree;
        this.cache = cache;
    }

    public String getData(String date,long lat, long lon){

        String choosenGribFilePath = tree.getFilePathByUnixTime();

        if(!isGribFileContainedInCache(choosenGribFilePath)){
            cache.put(choosenGribFilePath, GribFile.newGribFile(choosenGribFilePath,Job.VARIABLES_TO_BE_INTEGRATED));
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
