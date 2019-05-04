package com.github.nkoutroumanis.checkSpatialInfo;

import com.github.nkoutroumanis.FilesParse;
import com.github.nkoutroumanis.Parser;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

public final class CheckSpatialInfo implements FilesParse {

    private final Parser parser;

    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...

    private final String separator;

    private Set<String> filesWithErrors;

    private long numberOfRecords = 0;

    private double maxx = Integer.MIN_VALUE;
    private double minx = Integer.MAX_VALUE;
    private double maxy = Integer.MIN_VALUE;
    private double miny = Integer.MAX_VALUE;

    public static class Builder {

        private final Parser parser;
        private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...

        private String separator = ";";


        public Builder(Parser parser, int numberOfColumnLongitude, int numberOfColumnLatitude) {

            this.parser = parser;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
        }

        public Builder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public CheckSpatialInfo build() {
            return new CheckSpatialInfo(this);
        }

    }

    private CheckSpatialInfo(Builder builder) {
        parser = builder.parser;

        numberOfColumnLatitude = builder.numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        numberOfColumnLongitude = builder.numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...

        separator = builder.separator;
    }


    @Override
    public void emptySpatialInformation(Path file, String line) {

        if (filesWithErrors.contains("Empty Spatial Information " + file.toString())) {
            filesWithErrors.add("Empty Spatial Information " + file.toString());
        } else {
            filesWithErrors.add("Empty Spatial Information " + file.toString());
        }
    }

    @Override
    public void outOfRangeSpatialInformation(Path file, String line) {

        if (filesWithErrors.contains("Out of Range Spatial Information " + file.toString())) {
            filesWithErrors.add("Out of Range Spatial Information " + file.toString());
        } else {
            filesWithErrors.add("Out of Range Spatial Information " + file.toString());
        }
    }

    @Override
    public void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, double longitude, double latitude) {

        if (Double.compare(maxx, longitude) == -1) {
            maxx = longitude;
        }
        if (Double.compare(minx, longitude) == 1) {
            minx = longitude;
        }
        if (Double.compare(maxy, latitude) == -1) {
            maxy = latitude;
        }
        if (Double.compare(miny, latitude) == 1) {
            miny = latitude;
        }

        numberOfRecords++;

    }

    @Override
    public void lineWithError(Path file, String line) {
        if (filesWithErrors.contains("Lines with Errors " + file.toString())) {
            filesWithErrors.add("Lines with Errors " + file.toString());
        } else {
            filesWithErrors.add("Lines with Errors " + file.toString());
        }
    }

    public void exportTxt(String txtExportPath) {

        filesWithErrors = new HashSet<>();


        while (parser.hasNextLine()){

            String[] a = parser.nextLine();

            String line = a[0];
            String[] separatedLine = line.split(separator);

            if (Parser.empty.test(separatedLine[numberOfColumnLongitude - 1]) || Parser.empty.test(separatedLine[numberOfColumnLatitude - 1])) {
                continue;
            }

            double longitude = Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]);
            double latitude = Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]);


            if ((Double.compare(rectangle.getMaxx(), longitude) == 1) && (Double.compare(rectangle.getMinx(), longitude) == -1)
                    && (Double.compare(rectangle.getMaxy(), latitude) == 1) && (Double.compare(rectangle.getMiny(), latitude) == -1)) {
                numberOfRecordsInSpace2D++;
            }

            numberOfRecords++;

        }

        String fileName = "SpatialFilesInfo.txt";

        fileOutput.out("Files With Errors: ", fileName);
        filesWithErrors.forEach((s) -> fileOutput.out(s));
        fileOutput.out("\r\n", fileName);

        fileOutput.out("Formed Spatial Box: ", fileName);
        fileOutput.out("Max Longitude: " + maxx, fileName);
        fileOutput.out("Min Longitude: " + minx, fileName);
        fileOutput.out("Max Latitude: " + maxy, fileName);
        fileOutput.out("Min Latitude: " + miny, fileName);

        fileOutput.out("\r\n", fileName);
        fileOutput.out("All of the records are " + numberOfRecords, fileName);

        fileOutput.close();















        parse(filesPath, separator, filesExtension, numberOfColumnLongitude, numberOfColumnLatitude);

        //create Export Directory
        try {
            Files.createDirectories(Paths.get(txtExportPath));
        } catch (IOException e) {
            e.printStackTrace();
        }


        try (FileOutputStream fos = new FileOutputStream(txtExportPath + File.separator + "SpatialFilesInfo.txt", true);
             OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8"); BufferedWriter bw = new BufferedWriter(osw); PrintWriter pw = new PrintWriter(bw, true);) {

            pw.write("Files With Errors: " + "\r\n");
            filesWithErrors.forEach((s) -> pw.write(s + "\r\n"));
            pw.write("\r\n");

            pw.write("Formed Spatial Box: " + "\r\n");
            pw.write("Max Longitude: " + maxx + "\r\n");
            pw.write("Min Longitude: " + minx + "\r\n");
            pw.write("Max Latitude: " + maxy + "\r\n");
            pw.write("Min Latitude: " + miny + "\r\n");

            pw.write("\r\n");
            pw.write("All of the records are " + numberOfRecords + "\r\n");

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static Builder newCheckSpatioTemporalInfo(Parser parser, int numberOfColumnLongitude, int numberOfColumnLatitude) {
        return new CheckSpatialInfo.Builder(parser, numberOfColumnLongitude, numberOfColumnLatitude);
    }


}
