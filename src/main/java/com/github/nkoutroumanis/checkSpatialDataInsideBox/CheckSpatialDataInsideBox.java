package com.github.nkoutroumanis.checkSpatialDataInsideBox;

import com.github.nkoutroumanis.FilesParse;
import com.github.nkoutroumanis.histogram.Space2D;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

public final class CheckSpatialDataInsideBox implements FilesParse {

    private final String filesPath;

    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
    private final Space2D space2D;

    private final String filesExtension;
    private final String separator;

    private long numberOfRecords = 0;
    private long numberOfRecordsInSpace2D = 0;

    public static class Builder {

        private final String filesPath;
        private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        private final Space2D space2D;


        private String filesExtension = ".csv";
        private String separator = ";";


        public Builder(String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, Space2D space2D) {

            this.filesPath = filesPath;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
            this.space2D = space2D;
        }

        public Builder filesExtension(String filesExtension) {
            this.filesExtension = filesExtension;
            return this;
        }

        public Builder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public CheckSpatialDataInsideBox build() {
            return new CheckSpatialDataInsideBox(this);
        }

    }

    private CheckSpatialDataInsideBox(Builder builder) {
        filesPath = builder.filesPath;

        numberOfColumnLatitude = builder.numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        numberOfColumnLongitude = builder.numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        space2D = builder.space2D;

        filesExtension = builder.filesExtension;
        separator = builder.separator;
    }


    @Override
    public void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, double longitude, double latitude) {

        if((Double.compare(space2D.getMaxx(), longitude) == 1)&&(Double.compare(space2D.getMinx(), longitude) == -1)
                &&(Double.compare(space2D.getMaxy(), latitude) == 1)&&(Double.compare(space2D.getMiny(), latitude) == -1)){
            numberOfRecordsInSpace2D++;
        }

        numberOfRecords++;

    }

    public void exportTxt(String txtExportPath) {

        parse(filesPath, separator, filesExtension, numberOfColumnLongitude, numberOfColumnLatitude);

        //create Export Directory
        try {
            Files.createDirectories(Paths.get(txtExportPath));
        } catch (IOException e) {
            e.printStackTrace();
        }


        try (FileOutputStream fos = new FileOutputStream(txtExportPath + File.separator + "Spatial_Box_Info.txt", true);
             OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8"); BufferedWriter bw = new BufferedWriter(osw); PrintWriter pw = new PrintWriter(bw, true);) {

            pw.write("In the Spatial Box with: " + "\r\n");
            pw.write("maxLon: " + space2D.getMaxx() + "\r\n");
            pw.write("minLon: " + space2D.getMinx() + "\r\n");
            pw.write("maxLat: " + space2D.getMaxy() + "\r\n");
            pw.write("minLat: " + space2D.getMiny() + "\r\n");

            pw.write("There are " + numberOfRecordsInSpace2D + " records" + "\r\n");

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

    public static Builder newCheckSpatioTemporalInfo(String filesPath, int numberOfColumnLongitude, int numberOfColumnLatitude, Space2D space2D) {
        return new CheckSpatialDataInsideBox.Builder(filesPath, numberOfColumnLongitude, numberOfColumnLatitude, space2D);
    }


}
