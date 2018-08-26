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
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public final class SingleNodeJob {

    public static final String FILES_PATH = "/Users/nicholaskoutroumanis/Downloads";
    public static final String FILES_EXPORT_PATH = "/Users/nicholaskoutroumanis/Desktop/";
    public static final String FILES_EXTENSION = ".csv";
    public static final String SEPARATOR = ";";
    public static final int NUMBER_OF_COLULMN_DATE = 4;//1 if the 1st column represents the date, 2 if the 2nd column...
    public static final int NUMBER_OF_COLULMN_LATITUDE = 9;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    public static final int NUMBER_OF_COLULMN_LONGITUDE = 8;//1 if the 1st column represents the longitude, 2 if the 2nd column...
    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd-MM-yyyy");

    public static final String GRIB_FILES_FOLDER_PATH = "m";
    public static final String GRIB_FILES_EXTENSION = "grb2";

    public static final int LRUCACHE_MAX_ENTRIES = 3;

    private static final String[] VARIABLES = {"","",""};
    public static final List<String> VARIABLES_TO_BE_INTEGRATED = Collections.unmodifiableList(Arrays.asList(VARIABLES));

    public static void main(String args[]) {

        final LRUCacheManager manager = LRUCacheManager.newLRUCacheManager(GribFilesTree.INSTANCE, LRUCache.newLRUCache(LRUCACHE_MAX_ENTRIES));

        try (Stream<Path> stream = Files.walk(Paths.get(FILES_PATH)).filter(path->path.getFileName().toString().endsWith(".txt")).collect(toList()).parallelStream()) {

            stream.forEach((path) -> {

                try (Stream<String> innerStream = Files.lines(path);
                     FileOutputStream fos = new FileOutputStream(FILES_EXPORT_PATH +path.getFileName().toString(), true);
                     OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8");
                     BufferedWriter bw = new BufferedWriter(osw);
                     PrintWriter pw = new PrintWriter(bw, true)) {

                    innerStream.forEach( line -> {

                        String[] separatedLine = line.split(SEPARATOR);

                        try {
                            String dataToBeIntegrated = manager.getData(DATE_FORMAT.parse(separatedLine[NUMBER_OF_COLULMN_DATE-1]),Long.parseLong(separatedLine[NUMBER_OF_COLULMN_LATITUDE-1]),Long.parseLong(separatedLine[NUMBER_OF_COLULMN_LONGITUDE-1]));
                            pw.write(line + dataToBeIntegrated + "\r\n") ;

                        } catch (ParseException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                            }
                    );
                } catch (IOException ex) {
                    Logger.getLogger("INNER: "+SingleNodeJob.class.getName()).log(Level.SEVERE, null, ex);
                }
            });
        } catch (IOException ex) {
            Logger.getLogger("OUTER: "+SingleNodeJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
