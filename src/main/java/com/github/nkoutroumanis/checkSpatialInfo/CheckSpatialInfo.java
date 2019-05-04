package com.github.nkoutroumanis.checkSpatialInfo;

import com.github.nkoutroumanis.*;
import com.github.nkoutroumanis.weatherIntegrator.WeatherIntegrator;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;

public final class CheckSpatialInfo implements FilesParse {

    private final Parser parser;

    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...

    private final String separator;
    private final Rectangle rectangle;

    private Set<String> errorLines;
    private Set<String> emptySpatialInformation;
    private Set<String> spatialInformationOutOfRange;

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
        private Rectangle rectangle = Rectangle.newRectangle(-180,-90,180,90);


        public Builder(Parser parser, int numberOfColumnLongitude, int numberOfColumnLatitude) throws Exception {

            this.parser = parser;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
        }

        public Builder separator(String separator) {
            this.separator = separator;
            return this;
        }

        public Builder filter(Rectangle rectangle){
            this.rectangle = rectangle;
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
        rectangle = builder.rectangle;
    }


//    @Override
//    public void emptySpatialInformation(Path file, String line) {
//
//        if (filesWithErrors.contains("Empty Spatial Information " + file.toString())) {
//            filesWithErrors.add("Empty Spatial Information " + file.toString());
//        } else {
//            filesWithErrors.add("Empty Spatial Information " + file.toString());
//        }
//    }
//
//    @Override
//    public void outOfRangeSpatialInformation(Path file, String line) {
//
//        if (filesWithErrors.contains("Out of Range Spatial Information " + file.toString())) {
//            filesWithErrors.add("Out of Range Spatial Information " + file.toString());
//        } else {
//            filesWithErrors.add("Out of Range Spatial Information " + file.toString());
//        }
//    }

//    @Override
//    public void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, double longitude, double latitude) {
//
//        if (Double.compare(maxx, longitude) == -1) {
//            maxx = longitude;
//        }
//        if (Double.compare(minx, longitude) == 1) {
//            minx = longitude;
//        }
//        if (Double.compare(maxy, latitude) == -1) {
//            maxy = latitude;
//        }
//        if (Double.compare(miny, latitude) == 1) {
//            miny = latitude;
//        }
//
//        numberOfRecords++;
//
//    }

//    @Override
//    public void lineWithError(Path file, String line) {
//        if (filesWithErrors.contains("Lines with Errors " + file.toString())) {
//            filesWithErrors.add("Lines with Errors " + file.toString());
//        } else {
//            filesWithErrors.add("Lines with Errors " + file.toString());
//        }
//    }

    public void exportInfo(FileOutput fileOutput) throws IOException {

        errorLines = new HashSet<>();
        emptySpatialInformation = new HashSet<>();
        spatialInformationOutOfRange = new HashSet<>();


        while (parser.hasNextLine()){

            String[] a = parser.nextLine();

            try{

                String line = a[0];
                String[] separatedLine = line.split(separator);

                if (Parser.empty.test(separatedLine[numberOfColumnLongitude - 1]) || Parser.empty.test(separatedLine[numberOfColumnLatitude - 1])) {

                    if(parser instanceof KafkaParser){
                        a[1] = a[1].substring(0, a[1].lastIndexOf("."));
                    }

                    if (!emptySpatialInformation.contains(a[1])) {
                        emptySpatialInformation.add(a[1]);
                    }
                    continue;
                }

                double longitude = Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]);
                double latitude = Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]);

                //filtering
                if(((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))){

                    if(parser instanceof KafkaParser){
                        a[1] = a[1].substring(0, a[1].lastIndexOf("."));
                    }

                    if (!spatialInformationOutOfRange.contains(a[1])) {
                        spatialInformationOutOfRange.add(a[1]);
                    }
                    continue;
                }

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
            catch(ArrayIndexOutOfBoundsException | NumberFormatException e){

                if(parser instanceof KafkaParser){
                    a[1] = a[1].substring(0, a[1].lastIndexOf("."));
                }

                if (!errorLines.contains(a[1])) {
                    errorLines.add(a[1]);
                }

                continue;
            }

        }

        String fileName = "Spatial-Info.txt";

        fileOutput.out("Lines error at: ", fileName);
        errorLines.forEach((s) -> fileOutput.out(s, fileName));
        fileOutput.out("\r\n", fileName);

        fileOutput.out("Empty Spatial Information at: ", fileName);
        emptySpatialInformation.forEach((s) -> fileOutput.out(s, fileName));
        fileOutput.out("\r\n", fileName);

        fileOutput.out("Spatial Information out of range at: ", fileName);
        spatialInformationOutOfRange.forEach((s) -> fileOutput.out(s, fileName));
        fileOutput.out("\r\n", fileName);

        fileOutput.out("Formed Spatial Box: ", fileName);
        fileOutput.out("Max Longitude: " + maxx, fileName);
        fileOutput.out("Min Longitude: " + minx, fileName);
        fileOutput.out("Max Latitude: " + maxy, fileName);
        fileOutput.out("Min Latitude: " + miny, fileName);

        fileOutput.out("\r\n", fileName);
        fileOutput.out("All of the records are " + numberOfRecords, fileName);

        fileOutput.close();














//
//        parse(filesPath, separator, filesExtension, numberOfColumnLongitude, numberOfColumnLatitude);
//
//        //create Export Directory
//        try {
//            Files.createDirectories(Paths.get(txtExportPath));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//
//        try (FileOutputStream fos = new FileOutputStream(txtExportPath + File.separator + "SpatialFilesInfo.txt", true);
//             OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8"); BufferedWriter bw = new BufferedWriter(osw); PrintWriter pw = new PrintWriter(bw, true);) {
//
//            pw.write("Files With Errors: " + "\r\n");
//            filesWithErrors.forEach((s) -> pw.write(s + "\r\n"));
//            pw.write("\r\n");
//
//            pw.write("Formed Spatial Box: " + "\r\n");
//            pw.write("Max Longitude: " + maxx + "\r\n");
//            pw.write("Min Longitude: " + minx + "\r\n");
//            pw.write("Max Latitude: " + maxy + "\r\n");
//            pw.write("Min Latitude: " + miny + "\r\n");
//
//            pw.write("\r\n");
//            pw.write("All of the records are " + numberOfRecords + "\r\n");
//
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }

    public static Builder newCheckSpatioTemporalInfo(Parser parser, int numberOfColumnLongitude, int numberOfColumnLatitude) throws Exception {
        return new CheckSpatialInfo.Builder(parser, numberOfColumnLongitude, numberOfColumnLatitude);
    }

}
