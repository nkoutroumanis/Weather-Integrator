package gr.ds.unipi.wi;

import gr.ds.unipi.wi.grib.GribFilesTree;
import gr.ds.unipi.wi.lru.LRUCache;
import gr.ds.unipi.wi.lru.LRUCacheManager;
import ucar.nc2.NetcdfFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class PreProcessing {

    public static void main(String args[]) {

        long start = System.currentTimeMillis();

        try {
            Stream<String> stream = Files.lines(Paths.get("./src/test/resources/weather-attributes/weather-attributes.txt"));

            Function<String, NetcdfFile> netcdfFileFunction = (path) -> {
                try {
                    return NetcdfFile.open(path);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            };

            LRUCacheManager.newLRUCacheManager(GribFilesTree.newGribFilesTree("./src/test/resources/gribFiles/grib003Files/", ".nc", netcdfFileFunction),
                    LRUCache.newLRUCache(1), true, Collections.unmodifiableList(stream.collect(Collectors.toList())), netcdfFileFunction);

            Runtime rt = Runtime.getRuntime();
            System.out.println("Approximation of used Memory: " + (rt.totalMemory() - rt.freeMemory()) / 1000000 + " MB");
            System.out.println("Elapsed Time: " + (System.currentTimeMillis() - start) / 1000d + " sec");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
