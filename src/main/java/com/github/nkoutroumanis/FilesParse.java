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

    default void afterLineParse() {

    }

    default void fileParse(Path filePath) {

    }

    default void emptySpatiotemporalInformation(Path file, String line) {

    }

    default void outOfRangeSpatialInformation(Path file, String line) {

    }

    default void parse(String filesPath, String separator, String filesExtension, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate) {

        try (Stream<Path> stream = Files.walk(Paths.get(filesPath)).filter(path -> path.getFileName().toString().endsWith(filesExtension))) {

            stream.forEach((path) -> {

                fileParse(path);

                try (Stream<String> innerStream = Files.lines(path)) {
                    //for each line
                    innerStream.forEach(line -> {


                        String[] separatedLine = line.split(separator);

                        if (FilesParse.empty.test(separatedLine[numberOfColumnLongitude - 1]) || FilesParse.empty.test(separatedLine[numberOfColumnLatitude - 1]) || FilesParse.empty.test(separatedLine[numberOfColumnDate - 1])) {
                            emptySpatiotemporalInformation(path, line);
                            return;

                        }

                        double longitude = Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]);
                        double latitude = Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]);

                        if (FilesParse.longitudeOutOfRange.test(longitude) || FilesParse.latitudeOutOfRange.test(latitude)) {
                            outOfRangeSpatialInformation(path, line);
                            return;
                        } else {
                            lineParse(line, separatedLine, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate, longitude, latitude);
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

    static final Predicate<Double> longitudeOutOfRange = (longitude) -> ((Double.compare(longitude, 180) == 1) || (Double.compare(longitude, -180) == -1));
    static final Predicate<Double> latitudeOutOfRange = (latitude) -> ((Double.compare(latitude, 90) == 1) || (Double.compare(latitude, -90) == -1));
    static final Predicate<String> empty = (s1) -> (s1.equals(""));
}
