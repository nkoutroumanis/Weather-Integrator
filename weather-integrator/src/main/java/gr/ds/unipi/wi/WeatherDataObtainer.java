package gr.ds.unipi.wi;

import gr.ds.unipi.wi.grib.GribFilesTree;
import gr.ds.unipi.wi.lru.LRUCache;
import gr.ds.unipi.wi.lru.LRUCacheManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import ucar.nc2.NetcdfFile;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

public final class WeatherDataObtainer {

    private final LRUCacheManager lruCacheManager;

    private WeatherDataObtainer(String gribFilesFolderPath, String gribFilesExtension, int lruCacheMaxEntries, boolean useIndex, List<String> variables) throws IOException, URISyntaxException {

        Function<String, NetcdfFile> netcdfFileFunction;
        GribFilesTree gribFilesTree;

        if (gribFilesFolderPath.startsWith("hdfs")) {
            netcdfFileFunction = (path) -> {

                URI uri = null;
                try {
                    uri = new URI(path);
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                }
                return org.dia.utils.NetCDFUtils.loadDFSNetCDFDataSet("hdfs://" + uri.getAuthority() + "/", uri.getPath(), 1048576, true).getReferencedFile();
            };

            URI uri = new URI(gribFilesFolderPath);
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//            conf.set("fs.defaultFS", );
            gribFilesTree = GribFilesTree.newGribFilesTree(gribFilesFolderPath, gribFilesExtension, netcdfFileFunction, new Path(gribFilesFolderPath), FileSystem.get(new URI("hdfs://" + uri.getAuthority() + "/"), conf));
        } else {

            netcdfFileFunction = (path) -> {
                try {
                    return NetcdfFile.open(path);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            };

            gribFilesTree = GribFilesTree.newGribFilesTree(gribFilesFolderPath, gribFilesExtension, netcdfFileFunction);

        }

        lruCacheManager = LRUCacheManager.newLRUCacheManager(gribFilesTree,
                LRUCache.newLRUCache(lruCacheMaxEntries), useIndex, variables, netcdfFileFunction);
    }

    public static WeatherDataObtainer newWeatherDataObtainer(String gribFilesFolderPath, String gribFilesExtension, int lruCacheMaxEntries, boolean useIndex, List<String> variables) throws IOException, URISyntaxException {
        return new WeatherDataObtainer(gribFilesFolderPath, gribFilesExtension, lruCacheMaxEntries, useIndex, variables);
    }

    public List<Object> obtainAttributes(double longitude, double latitude, Date date) throws IOException {
        return lruCacheManager.getData(date, latitude, longitude);
    }

}
