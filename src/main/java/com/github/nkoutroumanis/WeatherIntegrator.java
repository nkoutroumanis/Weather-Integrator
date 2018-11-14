package com.github.nkoutroumanis;

import com.github.nkoutroumanis.grib.GribFilesTree;
import com.github.nkoutroumanis.lru.LRUCache;
import com.github.nkoutroumanis.lru.LRUCacheManager;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public final class WeatherIntegrator {

    private final String filesPath;
    private final String filesExportPath;
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
                LRUCache.newLRUCache(lruCacheMaxEntries), useIndex, Collections.unmodifiableList(builder.variables), separator);
    }

    private void clearExportingDirectory() {
        //delete existing exported files on the export path
        if (Files.exists(Paths.get(filesExportPath))) {
            Stream.of(new File(filesExportPath).listFiles()).filter((file -> file.toString().endsWith(filesExtension))).forEach(File::delete);
        }
    }

    public void integrateData() {

//        MongoCredential credential = MongoCredential.createCredential("myUserAdmin", "test", "abc123".toCharArray());
//        MongoClientOptions options = MongoClientOptions.builder()/*.sslEnabled(true)*/.build();
//        MongoClient mongoClient = new MongoClient(new ServerAddress("83.216.102.163", 28017), credential, options);
//        MongoCollection m = mongoClient.getDatabase("test").getCollection("geoPoints");
//
//
//        List<Long> times = new ArrayList<>();
//        List<String> fileswithProblem = new ArrayList<String>();
//        List<String> cordinatesProblem = new ArrayList<>();

        int filesPathLength = filesPath.length();

        int cellsInXAxis = 10000;
        int cellsInYAxis = 10000;

        double x = 60D / cellsInXAxis;
        double y = 149D / cellsInYAxis;

        System.out.println(x);
        System.out.println(y);

        int[] cell = new int[cellsInXAxis * cellsInYAxis];


        //create Export Directory
        try {
            Files.createDirectories(Paths.get(filesExportPath));
        } catch (IOException e) {
            e.printStackTrace();
        }

        //create subdirectories in Export Directory if exist
        try (Stream<String> stream = Files.walk(Paths.get(filesPath)).filter(path -> path.getFileName().toString().endsWith(filesExtension)).map(p -> p.getParent().toString().substring(filesPathLength)).distinct()) {

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

        //for each file do data integration
        try (Stream<Path> stream = Files.walk(Paths.get(filesPath)).filter(path -> path.getFileName().toString().endsWith(filesExtension))) {

            stream.forEach((path) -> {

                List<Document> docs = new ArrayList<>();

                try (Stream<String> innerStream = Files.lines(path);
                     FileOutputStream fos = new FileOutputStream(filesExportPath + File.separator + path.toString().substring(filesPathLength + 1), true);
                     OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8");
                     BufferedWriter bw = new BufferedWriter(osw);
                     PrintWriter pw = new PrintWriter(bw, true)) {

                    //for each line
                    innerStream.forEach(line -> {

                                long t1 = System.currentTimeMillis();

                                JobUsingIndex.numberofRows++;

                                String[] separatedLine = line.split(separator);

                                if ((separatedLine[numberOfColumnDate - 1].isEmpty() || separatedLine[numberOfColumnLatitude - 1].isEmpty() || separatedLine[numberOfColumnLongitude - 1].isEmpty()) || (separatedLine[numberOfColumnDate - 1].equals("") || separatedLine[numberOfColumnLatitude - 1].equals("") || separatedLine[numberOfColumnLongitude - 1].equals(""))) {
                                    //pw.write(line + ";;;;;;;;;;;;;" + "\r\n");
//                                    if (!fileswithProblem.contains(path.toString())) {
//                                        fileswithProblem.add(path.toString());
//                                    }

                                } else if ((Float.compare(Float.parseFloat(separatedLine[numberOfColumnLongitude - 1]), 180) == 1) || (Float.compare(Float.parseFloat(separatedLine[numberOfColumnLongitude - 1]), -180) == -1) || (Float.compare(Float.parseFloat(separatedLine[numberOfColumnLatitude - 1]), 90) == 1) || (Float.compare(Float.parseFloat(separatedLine[numberOfColumnLatitude - 1]), -90) == -1)) {

                                    System.out.println("entopistikan lathos sintetagmenes LONGITUDE:" + Float.parseFloat(separatedLine[numberOfColumnLongitude - 1]) + " LATITUDE:" + Float.parseFloat(separatedLine[numberOfColumnLatitude - 1]) + " " + path.toString());

//                                    if (!cordinatesProblem.contains(path.toString())) {
//                                        cordinatesProblem.add(path.toString());
//                                    }

                                } else {

                                    try {

                                        //docs.add( new Document("objectId", separatedLine[0]).append("coordinates", Arrays.asList(Float.parseFloat(separatedLine[numberOfColumnLongitude - 1]), Float.parseFloat(separatedLine[numberOfColumnLatitude - 1]))).append("date",dateFormat.parse(separatedLine[numberOfColumnDate - 1])));

                                        Document embeddedDoc = new Document("type", "Point").append("coordinates", Arrays.asList(Float.parseFloat(separatedLine[numberOfColumnLongitude - 1]), Float.parseFloat(separatedLine[numberOfColumnLatitude - 1])));
                                        docs.add(new Document("objectId", separatedLine[0]).append("location", embeddedDoc).append("date", dateFormat.parse(separatedLine[numberOfColumnDate - 1])));


                                        int xc = (int) (Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]) / x);

                                        int yc = (int) (Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]) / y);

                                        int k = xc + (yc * cellsInXAxis);

                                        cell[k] = cell[k] + 1;


                                    } catch (ParseException e) {
                                        e.printStackTrace();
                                    }

//
//                                    try {
//                                        String dataToBeIntegrated = lruCacheManager.getData(dateFormat.parse(separatedLine[numberOfColumnDate - 1]), Float.parseFloat(separatedLine[numberOfColumnLatitude - 1]), Float.parseFloat(separatedLine[numberOfColumnLongitude - 1]));
//                                        pw.write(line + dataToBeIntegrated + "\r\n");
//
//                                    } catch (ParseException e) {
//                                        e.printStackTrace();
//                                    } catch (IOException e) {
//                                        e.printStackTrace();
//                                    }
                                }
//                                times.add(System.currentTimeMillis() - t1);
                            }
                    );
                    if (docs.size() > 0) {
//                            m.insertMany(docs);
                    } else {
                        System.out.print(path);
                    }


                } catch (IOException ex) {
                    Logger.getLogger(JobUsingIndex.class.getName()).log(Level.SEVERE, null, ex);
                }
            });
        } catch (
                IOException ex) {
            Logger.getLogger(JobUsingIndex.class.getName()).log(Level.SEVERE, null, ex);
        }

//        fileswithProblem.stream().forEach(System.out::println);
//
//
//        cordinatesProblem.stream().forEach(System.out::println);
//
//        System.out.println("Average Time per Record: " + times.stream().
//
//                mapToLong(Long::longValue).
//
//                average());


        try {
            FileOutputStream file = new FileOutputStream("/home/nikolaos/Desktop/histogram.dat");
            for (int i = 0; i < cell.length; i++)
                file.write(cell[i]);
            file.close();
        } catch (IOException e) {
            System.out.println("Error - " + e.toString());
        }


    }

    public static Builder newWeatherIntegrator(String filesPath, String filesExportPath, String gribFilesFolderPath, int numberOfColumnDate,
                                               int numberOfColumnLatitude, int numberOfColumnLongitude, String dateFormat, List<String> variables) {
        return new WeatherIntegrator.Builder(filesPath, filesExportPath, gribFilesFolderPath, numberOfColumnDate,
                numberOfColumnLatitude, numberOfColumnLongitude, dateFormat, variables);
    }


}
