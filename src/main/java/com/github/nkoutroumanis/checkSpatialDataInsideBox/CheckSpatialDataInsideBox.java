package com.github.nkoutroumanis.checkSpatialDataInsideBox;

import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.datasources.Datasource;
import com.github.nkoutroumanis.Rectangle;

import java.io.IOException;

public final class CheckSpatialDataInsideBox {

    private final Datasource parser;

    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
    private final Rectangle rectangle;

    private final String separator;

    private long numberOfRecords = 0;
    private long numberOfRecordsInSpace2D = 0;

    public static class Builder {

        private final Datasource parser;
        private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        private final Rectangle rectangle;

        private String separator = ";";

        public Builder(Datasource parser, int numberOfColumnLongitude, int numberOfColumnLatitude, Rectangle rectangle) {

            this.parser = parser;
            this.numberOfColumnLatitude = numberOfColumnLatitude;
            this.numberOfColumnLongitude = numberOfColumnLongitude;
            this.rectangle = rectangle;
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
        parser = builder.parser;

        numberOfColumnLatitude = builder.numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
        numberOfColumnLongitude = builder.numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
        rectangle = builder.rectangle;

        separator = builder.separator;
    }


//    @Override
//    public void lineParse(String line, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, double longitude, double latitude) {
//
//        if ((Double.compare(space2D.getMaxx(), longitude) == 1) && (Double.compare(space2D.getMinx(), longitude) == -1)
//                && (Double.compare(space2D.getMaxy(), latitude) == 1) && (Double.compare(space2D.getMiny(), latitude) == -1)) {
//            numberOfRecordsInSpace2D++;
//        }
//
//        numberOfRecords++;
//
//    }

    public void exportInfo(FileOutput fileOutput) throws IOException {

        while (parser.hasNextLine()) {

            try {
                String[] a = parser.nextLine();

                String line = a[0];
                String[] separatedLine = line.split(separator);

                if (Datasource.empty.test(separatedLine[numberOfColumnLongitude - 1]) || Datasource.empty.test(separatedLine[numberOfColumnLatitude - 1])) {
                    continue;
                }

                double longitude = Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]);
                double latitude = Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]);


                if ((Double.compare(rectangle.getMaxx(), longitude) == 1) && (Double.compare(rectangle.getMinx(), longitude) == -1)
                        && (Double.compare(rectangle.getMaxy(), latitude) == 1) && (Double.compare(rectangle.getMiny(), latitude) == -1)) {
                    numberOfRecordsInSpace2D++;
                }

                numberOfRecords++;
            } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
                continue;
            }

        }

        String fileName = "Spatial-Box-Info.txt";

        fileOutput.out("In the Spatial Box with", fileName);
        fileOutput.out("maxLon: " + rectangle.getMaxx(), fileName);
        fileOutput.out("minLon: " + rectangle.getMinx(), fileName);
        fileOutput.out("maxLat: " + rectangle.getMaxy(), fileName);
        fileOutput.out("minLat: " + rectangle.getMiny(), fileName);

        fileOutput.out("There are " + numberOfRecordsInSpace2D + " records", fileName);
        fileOutput.out("\r\n", fileName);
        fileOutput.out("All of the records are " + numberOfRecords, fileName);

        fileOutput.close();

//
//
//
//
//
//
//
//
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
//        try (FileOutputStream fos = new FileOutputStream(txtExportPath + File.separator + "Spatial_Box_Info.txt", true);
//             OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8"); BufferedWriter bw = new BufferedWriter(osw); PrintWriter pw = new PrintWriter(bw, true);) {
//
//            pw.write("In the Spatial Box with" + "\r\n");
//            pw.write("maxLon: " + space2D.getMaxx() + "\r\n");
//            pw.write("minLon: " + space2D.getMinx() + "\r\n");
//            pw.write("maxLat: " + space2D.getMaxy() + "\r\n");
//            pw.write("minLat: " + space2D.getMiny() + "\r\n");
//
//            pw.write("There are " + numberOfRecordsInSpace2D + " records" + "\r\n");
//
//            pw.write("\r\n");
//            pw.write("All of the records are " + numberOfRecords + "\r\n");
//
//
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    }

    public static Builder newCheckSpatioTemporalInfo(Datasource parser, int numberOfColumnLongitude, int numberOfColumnLatitude, Rectangle rectangle) {
        return new CheckSpatialDataInsideBox.Builder(parser, numberOfColumnLongitude, numberOfColumnLatitude, rectangle);
    }


}
