package com.github.nkoutroumanis.checkSpatialDataInsideBox;

import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.parsers.Record;
import com.github.nkoutroumanis.parsers.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;

public final class CheckSpatialDataInsideBox {

    private static final Logger logger = LoggerFactory.getLogger(CheckSpatialDataInsideBox.class);

    private final RecordParser recordParser;
    private final Rectangle rectangle;

    private long numberOfRecords = 0;
    private long numberOfRecordsInSpace2D = 0;

    private CheckSpatialDataInsideBox(Builder builder) {
        recordParser = builder.recordParser;
        rectangle = builder.rectangle;
    }

    public static Builder newCheckSpatioTemporalInfo(RecordParser parser, Rectangle rectangle) {
        return new CheckSpatialDataInsideBox.Builder(parser, rectangle);
    }

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

    }

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


}
