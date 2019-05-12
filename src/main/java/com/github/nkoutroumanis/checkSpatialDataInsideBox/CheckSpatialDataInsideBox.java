package com.github.nkoutroumanis.checkSpatialDataInsideBox;

import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.datasources.Datasource;
import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.parsers.Record;
import com.github.nkoutroumanis.parsers.RecordParser;
import com.github.nkoutroumanis.weatherIntegrator.WeatherIntegrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

public final class CheckSpatialDataInsideBox {

    private static final Logger logger = LoggerFactory.getLogger(CheckSpatialDataInsideBox.class);

    private final RecordParser recordParser;
    private final Rectangle rectangle;

    private long numberOfRecords = 0;
    private long numberOfRecordsInSpace2D = 0;

    public static class Builder {

        private final RecordParser recordParser;
        private final Rectangle rectangle;

        public Builder(RecordParser recordParser, Rectangle rectangle) {

            this.recordParser = recordParser;
            this.rectangle = rectangle;
        }

        public CheckSpatialDataInsideBox build() {
            return new CheckSpatialDataInsideBox(this);
        }

    }

    private CheckSpatialDataInsideBox(Builder builder) {
        recordParser = builder.recordParser;
        rectangle = builder.rectangle;
    }


//    @Override
//    public void lineParse(String lineWithMeta, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, double longitude, double latitude) {
//
//        if ((Double.compare(space2D.getMaxx(), longitude) == 1) && (Double.compare(space2D.getMinx(), longitude) == -1)
//                && (Double.compare(space2D.getMaxy(), latitude) == 1) && (Double.compare(space2D.getMiny(), latitude) == -1)) {
//            numberOfRecordsInSpace2D++;
//        }
//
//        numberOfRecords++;
//
//    }

    public void exportInfo(FileOutput fileOutput) throws IOException, ParseException {

        while (recordParser.hasNextRecord()) {


            Record record = recordParser.nextRecord();

            try {

                double longitude = Double.parseDouble(recordParser.getLongitude(record));
                double latitude = Double.parseDouble(recordParser.getLatitude(record));

                if ((Double.compare(rectangle.getMaxx(), longitude) == 1) && (Double.compare(rectangle.getMinx(), longitude) == -1)
                        && (Double.compare(rectangle.getMaxy(), latitude) == 1) && (Double.compare(rectangle.getMiny(), latitude) == -1)) {
                    numberOfRecordsInSpace2D++;
                }

                numberOfRecords++;

            } catch (NumberFormatException e) {
                logger.warn("Spatial information of record can not be parsed {} \nLine {}", e, record.getMetadata());
                continue;
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.warn("Record is incorrect {} \nLine {}", e, record.getMetadata());
                continue;
            }





//            try {
//                String[] a = parser.nextLine();
//
//                String line = a[0];
//                String[] separatedLine = line.split(separator);
//
//                if (Datasource.empty.test(separatedLine[numberOfColumnLongitude - 1]) || Datasource.empty.test(separatedLine[numberOfColumnLatitude - 1])) {
//                    continue;
//                }
//
//                double longitude = Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]);
//                double latitude = Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]);
//
//
//                if ((Double.compare(rectangle.getMaxx(), longitude) == 1) && (Double.compare(rectangle.getMinx(), longitude) == -1)
//                        && (Double.compare(rectangle.getMaxy(), latitude) == 1) && (Double.compare(rectangle.getMiny(), latitude) == -1)) {
//                    numberOfRecordsInSpace2D++;
//                }
//
//                numberOfRecords++;
//            } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
//                continue;
//            }

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

    public static Builder newCheckSpatioTemporalInfo(RecordParser parser, Rectangle rectangle) {
        return new CheckSpatialDataInsideBox.Builder(parser, rectangle);
    }


}
