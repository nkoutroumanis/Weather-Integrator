package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.FilesParse;
import com.github.nkoutroumanis.weatherIntegrator.grib.GribFilesTree;
import com.github.nkoutroumanis.weatherIntegrator.lru.LRUCache;
import com.github.nkoutroumanis.weatherIntegrator.lru.LRUCacheManager;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

public final class WeatherIntegrator implements FilesParse {

    private final String filesPath;

    private final String gribFilesFolderPath;
    private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
    private final DateFormat dateFormat;

    private final String filesExtension;
    private final String gribFilesExtension;
    private final String separator;
    private final int lruCacheMaxEntries;
    private final boolean useIndex;

    private final LRUCacheManager lruCacheManager;

    private String filesExportPath;
    private FileOutputStream fos;
    private OutputStreamWriter osw;
    private BufferedWriter bw;
    private PrintWriter pw;

    public static double TEMPORARY_POINTER1 = 0;
    public static double TEMPORARY_POINTER2 = 0;

    public static class Builder {

        private final String filesPath;
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


        public Builder(String filesPath, String gribFilesFolderPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat, List<String> variables) {

            this.filesPath = filesPath;
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

        public WeatherIntegrator build() {
            return new WeatherIntegrator(this);
        }

    }

    private WeatherIntegrator(Builder builder) {
        filesPath = builder.filesPath;
        gribFilesFolderPath = builder.gribFilesFolderPath;
        numberOfColumnDate = builder.numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
        numberOfColumnLatitude = builder.numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        numberOfColumnLongitude = builder.numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        dateFormat = builder.dateFormat;

        filesExtension = builder.filesExtension;
        gribFilesExtension = builder.gribFilesExtension;
        separator = builder.separator;
        lruCacheMaxEntries = builder.lruCacheMaxEntries;
        useIndex = builder.useIndex;

        lruCacheManager = LRUCacheManager.newLRUCacheManager(GribFilesTree.newGribFilesTree(gribFilesFolderPath, gribFilesExtension),
                LRUCache.newLRUCache(lruCacheMaxEntries), useIndex, builder.variables, separator);
    }

    private void clearExportingDirectory() {
        //delete existing exported files on the export path
        if (Files.exists(Paths.get(filesExportPath))) {
            Stream.of(new File(filesExportPath).listFiles()).filter((file -> file.toString().endsWith(filesExtension))).forEach(File::delete);
        }
    }

    @Override
    public void fileParse(Path filePath) {

        try {
            fos = new FileOutputStream(filesExportPath + File.separator + filePath.toString().substring(filesPath.length() + 1), true);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            osw = new OutputStreamWriter(fos, "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        bw = new BufferedWriter(osw);
        pw = new PrintWriter(bw, true);
    }

    @Override
    public void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, double longitude, double latitude) {
        try {
            String dataToBeIntegrated = lruCacheManager.getData(dateFormat.parse(separatedLine[numberOfColumnDate - 1]), latitude, longitude);
            pw.write(line + dataToBeIntegrated + "\r\n");

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void emptySpatiotemporalInformation(Path file, String line) {
        pw.write(line + new String(new char[lruCacheManager.getNumberOfVariables()]).replace("\0", separator) + "\r\n");

    }

    @Override
    public void outOfRangeSpatialInformation(Path file, String line) {
        pw.write(line + new String(new char[lruCacheManager.getNumberOfVariables()]).replace("\0", separator) + "\r\n");

    }

    @Override
    public void afterLineParse() {
        try {

            pw.close();
            bw.close();
            osw.close();
            fos.close();
//            fos.close();
//            osw.close();
//            bw.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

//        pw.close();

    }

    public void integrateData(String filesExportPath) {

        this.filesExportPath = filesExportPath;

        clearExportingDirectory();

        //create Export Directory
        try {
            Files.createDirectories(Paths.get(filesExportPath));
        } catch (IOException e) {
            e.printStackTrace();
        }

        //create subdirectories in Export Directory if exist
        try (Stream<String> stream = Files.walk(Paths.get(filesPath)).filter(path -> path.getFileName().toString().endsWith(filesExtension)).map(p -> p.getParent().toString().substring(filesPath.length())).distinct()) {

            stream.forEach(subdirectory ->
            {
                try {
                    Files.createDirectories(Paths.get(filesExportPath + subdirectory));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

        } catch (IOException e) {
            e.printStackTrace();
        }

        parse(filesPath, separator, filesExtension, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate);
    }

    public static Builder newWeatherIntegrator(String filesPath, String gribFilesFolderPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat, List<String> variables) {
        return new WeatherIntegrator.Builder(filesPath, gribFilesFolderPath, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate, dateFormat, variables);
    }


}
