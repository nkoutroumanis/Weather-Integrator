package com.github.nkoutroumanis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public interface FilesParse {

    default void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, double longitude, double latitude) {

    }

    default void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, double longitude, double latitude) {

    }

    default void afterLineParse() {

    }

    default void fileParse(Path filePath) {

    }

    default void emptySpatioTemporalInformation(Path file, String line) {

    }

    default void emptySpatialInformation(Path file, String line) {

    }

    default void outOfRangeSpatialInformation(Path file, String line) {

    }

    default void lineWithError(Path file, String line) {

    }

    //SpatioTemporal  Parsing
    default void parse(String filesPath, String separator, String filesExtension, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate) {
        try (Stream<Path> stream = Files.walk(Paths.get(filesPath)).filter(path -> path.getFileName().toString().endsWith(filesExtension))) {

            stream.forEach((path) -> {

                fileParse(path);

                try (Stream<String> innerStream = Files.lines(path)) {
                    //for each line
                    innerStream.forEach(line -> {


                        try {

                            String[] separatedLine = line.split(separator);

                            if (FilesParse.empty.test(separatedLine[numberOfColumnLongitude - 1]) || FilesParse.empty.test(separatedLine[numberOfColumnLatitude - 1]) || FilesParse.empty.test(separatedLine[numberOfColumnDate - 1])) {
                                emptySpatioTemporalInformation(path, line);
                                return;
                            }

                            double longitude = Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]);
                            double latitude = Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]);

                            if (FilesParse.longitudeOutOfRange.test(longitude) || FilesParse.latitudeOutOfRange.test(latitude)) {
                                outOfRangeSpatialInformation(path, line);
                                return;
                            }

//                            if (!(FilesParse.longitudeInGreekRegion.test(longitude) && FilesParse.latitudeInGreekRegion.test(latitude))) {
//                                outOfRangeSpatialInformation(path, line);
//                                return;
//                            }

                            else {
                                lineParse(line, separatedLine, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate, longitude, latitude);
                            }

                        } catch (ArrayIndexOutOfBoundsException e) {
                            lineWithError(path, line);
                        }
                    });

                    afterLineParse();

                } catch (IOException ex) {
                    Logger.getLogger(FilesParse.class.getName()).log(Level.SEVERE, null, ex);
                }

            });
        } catch (
                IOException ex) {
            Logger.getLogger(FilesParse.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    //Spatial Parsing
    default void parse(String filesPath, String separator, String filesExtension, int numberOfColumnLongitude, int numberOfColumnLatitude) {

        try (Stream<Path> stream = Files.walk(Paths.get(filesPath)).filter(path -> path.getFileName().toString().endsWith(filesExtension))) {

            stream.forEach((path) -> {

                fileParse(path);

                try (Stream<String> innerStream = Files.lines(path)) {
                    //for each line
                    innerStream.forEach(line -> {

                        try {

                            String[] separatedLine = line.split(separator);

                            if (FilesParse.empty.test(separatedLine[numberOfColumnLongitude - 1]) || FilesParse.empty.test(separatedLine[numberOfColumnLatitude - 1])) {
                                emptySpatialInformation(path, line);
                                return;
                            }

                            double longitude = Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]);
                            double latitude = Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]);

                            if (FilesParse.longitudeOutOfRange.test(longitude) || FilesParse.latitudeOutOfRange.test(latitude)) {
                                outOfRangeSpatialInformation(path, line);
                                return;
                            } else {
                                lineParse(line, separatedLine, numberOfColumnLongitude, numberOfColumnLatitude, longitude, latitude);
                            }

                        } catch (ArrayIndexOutOfBoundsException e) {
                            lineWithError(path, line);
                        }
                    });

                    afterLineParse();

                } catch (IOException ex) {
                    Logger.getLogger(FilesParse.class.getName()).log(Level.SEVERE, null, ex);
                }

            });
        } catch (
                IOException ex) {
            Logger.getLogger(FilesParse.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public static double harvesine(double lon1, double lat1, double lon2, double lat2) {

        double r = 6378.1;

        double f1 = Math.toRadians(lat1);
        double f2 = Math.toRadians(lat2);

        double df = Math.toRadians(lat2 - lat1);
        double dl = Math.toRadians(lon2 - lon1);

        double a = Math.sin(df / 2) * Math.sin(df / 2) + Math.cos(f1) * Math.cos(f2) * Math.sin(dl / 2) * Math.sin(dl / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return r * c;
    }

    static final Predicate<Double> longitudeOutOfRange = (longitude) -> ((Double.compare(longitude, 180) == 1) || (Double.compare(longitude, -180) == -1));
    static final Predicate<Double> latitudeOutOfRange = (latitude) -> ((Double.compare(latitude, 90) == 1) || (Double.compare(latitude, -90) == -1));

    static final Predicate<Double> longitudeInGreekRegion = (longitude) -> ((Double.compare(longitude, 26.6041955909) == -1) && (Double.compare(longitude, 20.1500159034) == 1));
    static final Predicate<Double> latitudeInGreekRegion = (latitude) -> ((Double.compare(latitude, 41.8269046087) == -1) && (Double.compare(latitude, 34.9199876979) == 1));

    static final Predicate<String> empty = (s1) -> (s1.trim().isEmpty());
}
