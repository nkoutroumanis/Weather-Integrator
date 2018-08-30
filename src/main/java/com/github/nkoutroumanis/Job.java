package com.github.nkoutroumanis;

import com.github.nkoutroumanis.grib.GribFilesTree;
import com.github.nkoutroumanis.lru.LRUCache;
import com.github.nkoutroumanis.lru.LRUCacheManager;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public final class Job {

    public static final String FILES_PATH = "/Users/nicholaskoutroumanis/Desktop/csv";
    public static final String FILES_EXPORT_PATH = "/Users/nicholaskoutroumanis/Desktop/folder/";
    public static final String FILES_EXTENSION = ".csv";
    public static final String SEPARATOR = ";";
    public static final int NUMBER_OF_COLULMN_DATE = 4;//1 if the 1st column represents the date, 2 if the 2nd column...
    public static final int NUMBER_OF_COLULMN_LATITUDE = 9;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    public static final int NUMBER_OF_COLULMN_LONGITUDE = 8;//1 if the 1st column represents the longitude, 2 if the 2nd column...
    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");

    public static final String GRIB_FILES_FOLDER_PATH = "./grib_files";
    public static final String GRIB_FILES_EXTENSION = "grb2";

    public static final int LRUCACHE_MAX_ENTRIES = 3;
    public static final boolean USE_INDEX = false;

    private static final String[] VARIABLES = {"Relative_humidity_height_above_ground","Temperature_height_above_ground"};
    public static final List<String> VARIABLES_TO_BE_INTEGRATED = Collections.unmodifiableList(Arrays.asList(VARIABLES));

    public static double TEMPORARY_POINTER1 = 0;
    public static double TEMPORARY_POINTER2 = 0;

    public static void main(String args[]) {

        //delete existing exported files on the export path
        Stream.of(new File(FILES_EXPORT_PATH).listFiles()).filter((file -> file.toString().endsWith(FILES_EXTENSION))).forEach(File::delete);

        final LRUCacheManager manager = LRUCacheManager.newLRUCacheManager(GribFilesTree.INSTANCE, LRUCache.newLRUCache(LRUCACHE_MAX_ENTRIES));

        long t1;

        t1 = System.currentTimeMillis();
        try (Stream<Path> stream = Files.walk(Paths.get(FILES_PATH)).filter(path->path.getFileName().toString().endsWith(FILES_EXTENSION))) {

            stream.forEach((path) -> {



                try (Stream<String> innerStream = Files.lines(path);
                     FileOutputStream fos = new FileOutputStream(FILES_EXPORT_PATH +path.getFileName().toString(), true);
                     OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8");
                     BufferedWriter bw = new BufferedWriter(osw);
                     PrintWriter pw = new PrintWriter(bw, true)) {

                    innerStream.forEach( line -> {

                        String[] separatedLine = line.split(SEPARATOR);


                        long t2 = 0;
                        try {
                            String dataToBeIntegrated = manager.getData(DATE_FORMAT.parse(separatedLine[NUMBER_OF_COLULMN_DATE-1]),Float.parseFloat(separatedLine[NUMBER_OF_COLULMN_LATITUDE-1]),Float.parseFloat(separatedLine[NUMBER_OF_COLULMN_LONGITUDE-1]));
                            pw.write(line + dataToBeIntegrated + "\r\n") ;

                        } catch (ParseException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                            }
                    );

                } catch (IOException ex) {
                    Logger.getLogger("INNER: "+Job.class.getName()).log(Level.SEVERE, null, ex);
                }
            });
        } catch (IOException ex) {
            Logger.getLogger("OUTER: "+Job.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.out.println("Average: "+ Job.TEMPORARY_POINTER1/Job.TEMPORARY_POINTER2);
        System.out.println("TIME ELAPSED: "+ (System.currentTimeMillis()-t1));

    }

}
