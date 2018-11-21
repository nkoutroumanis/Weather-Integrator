package com.github.nkoutroumanis.checkspatiotemporalinfo;

import com.github.nkoutroumanis.FilesParse;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public final class CheckSpatioTemporalInfo implements FilesParse {

    private final String filesPath;

    private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
    private final DateFormat dateFormat;

    private final String filesExtension;
    private final String separator;

    //private String txtExportPath;
    private Set<String> filesWithErrors;

    public static class Builder {

        private final String filesPath;
        private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
        private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        private final DateFormat dateFormat;

        private String filesExtension = ".csv";
        private String separator = ";";


        public Builder(String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat) {

            this.filesPath = filesPath;
            this.numberOfColumnDate = numberOfColumnDate;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
            this.dateFormat = new SimpleDateFormat(dateFormat);
        }

        public Builder filesExtension(String filesExtension) {
            this.filesExtension = filesExtension;
            return this;
        }

        public Builder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public CheckSpatioTemporalInfo build() {
            return new CheckSpatioTemporalInfo(this);
        }

    }

    private CheckSpatioTemporalInfo(Builder builder) {
        filesPath = builder.filesPath;

        numberOfColumnDate = builder.numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
        numberOfColumnLatitude = builder.numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        numberOfColumnLongitude = builder.numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        dateFormat = builder.dateFormat;

        filesExtension = builder.filesExtension;
        separator = builder.separator;
    }


    @Override
    public void emptySpatiotemporalInformation(Path file, String line) {

        if (filesWithErrors.contains("Empty Spatitemporal Information " + file.toString())) {
            filesWithErrors.add("Empty Spatiotemporal Information " + file.toString());
        } else {
            filesWithErrors.add("Empty Spatitemporal Information " + file.toString());
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

    public void exportTxt(String txtExportPath) {

        filesWithErrors = new HashSet<>();

        parse(filesPath, separator, filesExtension, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate);

        //create Export Directory
        try {
            Files.createDirectories(Paths.get(txtExportPath));
        } catch (IOException e) {
            e.printStackTrace();
        }


        try (FileOutputStream fos = new FileOutputStream(txtExportPath + File.separator + "SpatiotemporalFilesInfo.txt", true);
             OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8"); BufferedWriter bw = new BufferedWriter(osw); PrintWriter pw = new PrintWriter(bw, true);) {

            filesWithErrors.forEach((s) -> pw.write(s + "\r\n"));

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static Builder newCheckSpatioTemporalInfo(String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, String dateFormat) {
        return new CheckSpatioTemporalInfo.Builder(filesPath, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate, dateFormat);
    }


}
