package com.github.nkoutroumanis.checkSpatioTemporalInfo;

import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.datasources.KafkaDatasource;
import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.parsers.Record;
import com.github.nkoutroumanis.parsers.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public final class CheckSpatioTemporalInfo {

    private static final Logger logger = LoggerFactory.getLogger(CheckSpatioTemporalInfo.class);

    private final RecordParser recordParser;

    private final Rectangle rectangle;

    private Set<String> errorLines;
    //private Set<String> emptySpatialInformation;
    private Set<String> spatioTemporalInformationOutOfRange;

    private long numberOfRecords = 0;

    private double maxx = Integer.MIN_VALUE;
    private double minx = Integer.MAX_VALUE;
    private double maxy = Integer.MIN_VALUE;
    private double miny = Integer.MAX_VALUE;

    private CheckSpatioTemporalInfo(Builder builder) {
        recordParser = builder.recordParser;
        rectangle = builder.rectangle;
    }

    public static Builder newCheckSpatioTemporalInfo(RecordParser recordParser) throws Exception {
        return new CheckSpatioTemporalInfo.Builder(recordParser);
    }


//    @Override
//    public void emptySpatioTemporalInformation(Path file, String lineWithMeta) {
//
//        if (filesWithErrors.contains("Empty Spatitemporal Information " + file.toString())) {
//            filesWithErrors.add("Empty Spatiotemporal Information " + file.toString());
//        } else {
//            filesWithErrors.add("Empty Spatitemporal Information " + file.toString());
//        }
//    }

//    @Override
//    public void outOfRangeSpatialInformation(Path file, String lineWithMeta) {
//
//        if (filesWithErrors.contains("Out of Range Spatial Information " + file.toString())) {
//            filesWithErrors.add("Out of Range Spatial Information " + file.toString());
//        } else {
//            filesWithErrors.add("Out of Range Spatial Information " + file.toString());
//        }
//    }
//
//    @Override
//    public void lineParse(String lineWithMeta, String[] separatedLine, int numberOfColumnLongitude, int numberOfColumnLatitude, int numberOfColumnDate, double longitude, double latitude) {
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
//    public void lineWithError(Path file, String lineWithMeta) {
//        if (filesWithErrors.contains("Lines with Errors " + file.toString())) {
//            filesWithErrors.add("Lines with Errors " + file.toString());
//        } else {
//            filesWithErrors.add("Lines with Errors " + file.toString());
//        }
//    }

    public void exportInfo(FileOutput fileOutput) throws IOException, ParseException {

        errorLines = new HashSet<>();
        //emptySpatialInformation = new HashSet<>();
        spatioTemporalInformationOutOfRange = new HashSet<>();

        DateFormat dateFormat = new SimpleDateFormat(recordParser.getDateFormat());

        while (recordParser.hasNextRecord()) {

            Record record = recordParser.nextRecord();
            String lineMetaData = record.getMetadata();

            try {

                double longitude = Double.parseDouble(recordParser.getLongitude(record));
                double latitude = Double.parseDouble(recordParser.getLatitude(record));
                Date d = dateFormat.parse(recordParser.getDate(record));

                //filtering
                if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {

                    if (recordParser.getDatasource() instanceof KafkaDatasource) {
                        lineMetaData = lineMetaData.substring(0, lineMetaData.lastIndexOf("."));
                    }

                    if (!spatioTemporalInformationOutOfRange.contains(lineMetaData)) {
                        spatioTemporalInformationOutOfRange.add(lineMetaData);
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


//                if (Datasource.empty.test(separatedLine[numberOfColumnLongitude - 1]) || Datasource.empty.test(separatedLine[numberOfColumnLatitude - 1]) || Datasource.empty.test(separatedLine[numberOfColumnDate - 1])) {
//
//                    if (parser instanceof KafkaDatasource) {
//                        a[1] = a[1].substring(0, a[1].lastIndexOf("."));
//                    }
//
//                    if (!emptySpatialInformation.contains(a[1])) {
//                        emptySpatialInformation.add(a[1]);
//                    }
//                    continue;
//                }


            } catch (NumberFormatException | ParseException | ArrayIndexOutOfBoundsException e) {

                if ((e instanceof NumberFormatException) || (e instanceof ParseException)) {
                    logger.warn("Spatial information of record can not be parsed {} \nLine {}", e, record.getMetadata());
                } else {
                    logger.warn("Record is incorrect {} \nLine {}", e, record.getMetadata());
                }

                if (recordParser.getDatasource() instanceof KafkaDatasource) {
                    lineMetaData = lineMetaData.substring(0, lineMetaData.lastIndexOf("."));
                }

                if (!errorLines.contains(lineMetaData)) {
                    errorLines.add(lineMetaData);
                }
            }

//            catch (ArrayIndexOutOfBoundsException | NumberFormatException | ParseException e) {
//
//                if (parser instanceof KafkaDatasource) {
//                    a[1] = a[1].substring(0, a[1].lastIndexOf("."));
//                }
//
//                if (!errorLines.contains(a[1])) {
//                    errorLines.add(a[1]);
//                }
//
//                continue;
//            }

        }

        String fileName = "Spatio-temporal-Info.txt";

        fileOutput.out("Lines error at: ", fileName);
        errorLines.forEach((s) -> fileOutput.out(s, fileName));
        fileOutput.out("\r\n", fileName);

//        fileOutput.out("Empty Spatio-temporal Information at: ", fileName);
//        emptySpatialInformation.forEach((s) -> fileOutput.out(s, fileName));
//        fileOutput.out("\r\n", fileName);

        fileOutput.out("Spatial Information out of range at: ", fileName);
        spatioTemporalInformationOutOfRange.forEach((s) -> fileOutput.out(s, fileName));
        fileOutput.out("\r\n", fileName);

        fileOutput.out("Formed Spatial Box: ", fileName);
        fileOutput.out("Max Longitude: " + maxx, fileName);
        fileOutput.out("Min Longitude: " + minx, fileName);
        fileOutput.out("Max Latitude: " + maxy, fileName);
        fileOutput.out("Min Latitude: " + miny, fileName);

        fileOutput.out("\r\n", fileName);
        fileOutput.out("All of the records are " + numberOfRecords, fileName);

        fileOutput.close();


//        filesWithErrors = new HashSet<>();
//
//        parse(filesPath, separator, filesExtension, numberOfColumnLongitude, numberOfColumnLatitude, numberOfColumnDate);
//
//        //create Export Directory
//        try {
//            Files.createDirectories(Paths.get(txtExportPath));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//
//        try (FileOutputStream fos = new FileOutputStream(txtExportPath + File.separator + "SpatiotemporalFilesInfo.txt", true);
//             OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8"); BufferedWriter bw = new BufferedWriter(osw); PrintWriter pw = new PrintWriter(bw, true);) {
//
//            pw.write("Files With Errors:" + "\r\n");
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

    public static class Builder {

        private final RecordParser recordParser;

        private Rectangle rectangle = Rectangle.newRectangle(-180, -90, 180, 90);

        public Builder(RecordParser recordParser) throws Exception {
            this.recordParser = recordParser;
            this.rectangle = rectangle;
        }

        public Builder filter(Rectangle rectangle) {
            this.rectangle = rectangle;
            return this;
        }

        public CheckSpatioTemporalInfo build() {
            return new CheckSpatioTemporalInfo(this);
        }

    }


}
