package com.github.nkoutroumanis;

import com.github.nkoutroumanis.grib.GribFilesTree;
import com.github.nkoutroumanis.lru.LRUCache;
import com.github.nkoutroumanis.lru.LRUCacheManager;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import static java.util.stream.Collectors.toList;

public final class ParellelJob {

    public static final String INPUT_CSV_FILES_PATH = "/Volumes/NikosOrange/input";
    public static final String OUTPUT_CSV_FILES_PATH = "/Volumes/NikosOrange/output/";

    public static final String GRIB_FILES_FOLDER_PATH = "m";
    public static final String GRIB_FILES_EXTENSION = "grb2";
    public static final int LRUCACHE_MAX_ENTRIES = 3;

    private static final String[] VARIABLES = {"","",""};
    public static final List<String> VARIABLES_TO_BE_INTEGRATED = Collections.unmodifiableList(Arrays.asList(VARIABLES));
    public static final String SEPARATOR = ",";



    public static void main(String args[]) {

        //final LRUCacheManager manager = LRUCacheManager.newLRUCacheManager(GribFilesTree.INSTANCE, LRUCache.newLRUCache(LRUCACHE_MAX_ENTRIES));

        try (Stream<Path> stream = Files.walk(Paths.get(INPUT_CSV_FILES_PATH))) {

            stream.filter(path->path.getFileName().toString().endsWith(".txt")).collect(toList()).parallelStream().forEach((path) -> {

                try (Stream<String> innerStream = Files.lines(path);
                     FileOutputStream fos = new FileOutputStream(OUTPUT_CSV_FILES_PATH+path.getFileName().toString(), true);
                     OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8");
                     BufferedWriter bw = new BufferedWriter(osw);
                     PrintWriter pw = new PrintWriter(bw, true)) {





                    innerStream.forEach( line -> pw.write(line + "\r\n") );

                } catch (IOException ex) {
                    Logger.getLogger("INNER: "+ParellelJob.class.getName()).log(Level.SEVERE, null, ex);
                }

            });

        } catch (IOException ex) {
            Logger.getLogger("OUTER: "+ParellelJob.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
