package com.github.nkoutroumanis.checkSpatialInfo;

import com.github.nkoutroumanis.FilesParse;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

public final class CheckSpatialInfo implements FilesParse {

    private final String filesPath;

    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...

    private final String filesExtension;
    private final String separator;

    private Set<String> filesWithErrors;

    private double maxx = Integer.MIN_VALUE;
    private double minx = Integer.MAX_VALUE;
    private double maxy = Integer.MIN_VALUE;
    private double miny = Integer.MAX_VALUE;

    public static class Builder {

        private final String filesPath;
        private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...

        private String filesExtension = ".csv";
        private String separator = ";";


        public Builder(String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude) {

            this.filesPath = filesPath;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
        }

        public Builder filesExtension(String filesExtension) {
            this.filesExtension = filesExtension;
            return this;
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
        filesPath = builder.filesPath;

        numberOfColumnLatitude = builder.numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        numberOfColumnLongitude = builder.numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...

        filesExtension = builder.filesExtension;
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

        if(Double.compare(maxx, longitude) == -1){
            maxx = longitude;
        }
        if(Double.compare(minx, longitude) == 1){
            minx = longitude;
        }
        if(Double.compare(maxy, latitude) == -1){
            maxy = latitude;
        }
        if(Double.compare(miny, latitude) == 1){
            miny = latitude;
        }

    }

    @Override
    public void lineWithError(Path file, String line){
        if (filesWithErrors.contains("Lines with Errors " + file.toString())) {
            filesWithErrors.add("Lines with Errors " + file.toString());
        } else {
            filesWithErrors.add("Lines with Errors " + file.toString());
        }
    }


    public void exportTxt(String txtExportPath) {

        filesWithErrors = new HashSet<>();

        parse(filesPath, separator, filesExtension, numberOfColumnLongitude, numberOfColumnLatitude);

        //create Export Directory
        try {
            Files.createDirectories(Paths.get(txtExportPath));
        } catch (IOException e) {
            e.printStackTrace();
        }


        try (FileOutputStream fos = new FileOutputStream(txtExportPath + File.separator + "SpatialFilesInfo.txt", true);
             OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8"); BufferedWriter bw = new BufferedWriter(osw); PrintWriter pw = new PrintWriter(bw, true);) {

            pw.write("Files With Errors:" + "\r\n");
            filesWithErrors.forEach((s) -> pw.write(s + "\r\n"));
            pw.write("\r\n");

            pw.write("Spatial Box" + "\r\n");
            pw.write("Max Longitude" + maxx + "\r\n");
            pw.write("Min Longitude" + minx + "\r\n");
            pw.write("Max Latitude" + maxy + "\r\n");
            pw.write("Min Latitude" + miny + "\r\n");

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static Builder newCheckSpatioTemporalInfo(String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude) {
        return new CheckSpatialInfo.Builder(filesPath, numberOfColumnLongitude, numberOfColumnLatitude);
    }


}
