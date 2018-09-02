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

public class WeatherIntegrator {

    private final String filesPath;
    private final String filesExportPath;
    private final String gribFilesFolderPath;
    private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
    private final DateFormat dateFormat;
    //private final String[] variables;

    private final String filesExtension;
    private final String gribFilesExtension;
    private final String separator;
    private final int lruCacheMaxEntries;
    private final boolean useIndex;
    private final boolean clearExportingDirectory;

    private final LRUCacheManager lruCacheManager;

    public static double TEMPORARY_POINTER1 = 0;
    public static double TEMPORARY_POINTER2 = 0;

    public static class Builder {

        private final String filesPath;
        private final String filesExportPath;
        private final String gribFilesFolderPath;
        private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
        private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        private final DateFormat dateFormat;
        private final List<String> variables;

        private String filesExtension = ".csv";
        private String gribFilesExtension = ".grb2";
        private String separator = ";";
        private int lruCacheMaxEntries = 4;
        private boolean useIndex = false;
        private boolean clearExportingDirectory = false;

        public Builder(String filesPath, String filesExportPath, String gribFilesFolderPath, int numberOfColumnDate,
                       int numberOfColumnLatitude, int numberOfColumnLongitude, String dateFormat, List<String> variables) {

            this.filesPath = filesPath;
            this.filesExportPath = filesExportPath;
            this.gribFilesFolderPath = gribFilesFolderPath;
            this.numberOfColumnDate = numberOfColumnDate;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
            this.dateFormat = new SimpleDateFormat(dateFormat);
            this.variables = variables;
        }

        public Builder filesExtension(String filesExtension) {
            this.filesExtension = filesExtension;
            return this;
        }

        public Builder gribFilesExtension(String gribFilesExtension) {
            this.gribFilesExtension = gribFilesExtension;
            return this;
        }

        public Builder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public Builder lruCacheMaxEntries(int lruCacheMaxEntries) {
            this.lruCacheMaxEntries = lruCacheMaxEntries;
            return this;
        }

        public Builder useIndex() {
            this.useIndex = true;
            return this;
        }

        public Builder clearExportingFiles() {
            this.clearExportingDirectory = true;
            return this;
        }

        public WeatherIntegrator build() {
            return new WeatherIntegrator(this);
        }

    }

    private WeatherIntegrator(Builder builder) {
        filesPath = builder.filesPath;
        filesExportPath = builder.filesExportPath;
        gribFilesFolderPath = builder.gribFilesFolderPath;
        numberOfColumnDate = builder.numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
        numberOfColumnLatitude = builder.numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        numberOfColumnLongitude = builder.numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        dateFormat = builder.dateFormat;
        //variables = builder.variables;

        filesExtension = builder.filesExtension;
        gribFilesExtension = builder.gribFilesExtension;
        separator = builder.separator;
        lruCacheMaxEntries = builder.lruCacheMaxEntries;
        useIndex = builder.useIndex;
        clearExportingDirectory = builder.clearExportingDirectory;

        if (clearExportingDirectory) {
            clearExportingDirectory();
        }

        lruCacheManager = LRUCacheManager.newLRUCacheManager(GribFilesTree.newGribFilesTree(gribFilesFolderPath, gribFilesExtension),
                LRUCache.newLRUCache(lruCacheMaxEntries), useIndex, builder.variables, separator);
    }

    private void clearExportingDirectory() {
        //delete existing exported files on the export path
        Stream.of(new File(filesExportPath).listFiles()).filter((file -> file.toString().endsWith(filesExtension))).forEach(File::delete);
    }

    public void IntegrateData() {
        long t1;

        t1 = System.currentTimeMillis();
        try (Stream<Path> stream = Files.walk(Paths.get(filesPath)).filter(path -> path.getFileName().toString().endsWith(filesExtension))) {

            stream.forEach((path) -> {

                try (Stream<String> innerStream = Files.lines(path);
                     FileOutputStream fos = new FileOutputStream(filesExportPath + path.getFileName().toString(), true);
                     OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8");
                     BufferedWriter bw = new BufferedWriter(osw);
                     PrintWriter pw = new PrintWriter(bw, true)) {

                    innerStream.forEach(line -> {

                                String[] separatedLine = line.split(separator);


                                //long t2 = 0;
                                try {
                                    String dataToBeIntegrated = lruCacheManager.getData(dateFormat.parse(separatedLine[numberOfColumnDate - 1]), Float.parseFloat(separatedLine[numberOfColumnLatitude - 1]), Float.parseFloat(separatedLine[numberOfColumnLongitude - 1]));
                                    pw.write(line + dataToBeIntegrated + "\r\n");

                                } catch (ParseException e) {
                                    e.printStackTrace();
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                    );

                } catch (IOException ex) {
                    Logger.getLogger("INNER: " + Job.class.getName()).log(Level.SEVERE, null, ex);
                }
            });
        } catch (IOException ex) {
            Logger.getLogger("OUTER: " + Job.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.out.println("Average: " + WeatherIntegrator.TEMPORARY_POINTER1 / WeatherIntegrator.TEMPORARY_POINTER2);
        System.out.println("TIME ELAPSED: " + (System.currentTimeMillis() - t1));
    }

    public static Builder newWeatherIntegrator(String filesPath, String filesExportPath, String gribFilesFolderPath, int numberOfColumnDate,
                                               int numberOfColumnLatitude, int numberOfColumnLongitude, String dateFormat, List<String> variables) {
        return new WeatherIntegrator.Builder(filesPath, filesExportPath, gribFilesFolderPath, numberOfColumnDate,
                numberOfColumnLatitude, numberOfColumnLongitude, dateFormat, variables);
    }


}
