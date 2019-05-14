package com.github.nkoutroumanis.filesSampling;

import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.checkSpatialInfo.CheckSpatialInfo;
import com.github.nkoutroumanis.datasources.Datasource;
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
import java.util.Random;
import java.util.Set;

public class FilesSamping {

    private static final Logger logger = LoggerFactory.getLogger(FilesSamping.class);

    private final RecordParser recordParser;
    private final long samples;

    private final Rectangle rectangle;

    //private long numberOfRecords = 0;

    private double maxx = Integer.MIN_VALUE;
    private double minx = Integer.MAX_VALUE;
    private double maxy = Integer.MIN_VALUE;
    private double miny = Integer.MAX_VALUE;

    private FilesSamping(Builder builder) {
        recordParser = builder.recordParser;
        samples = builder.samples;
        rectangle = builder.rectangle;
    }

    public static Builder newFilesSamping(RecordParser recordParser, long samples) throws Exception {
        return new Builder(recordParser, samples);
    }

    public static class Builder {

        private final RecordParser recordParser;
        private final long samples;

        private Rectangle rectangle = Rectangle.newRectangle(-180, -90, 180, 90);

        public Builder(RecordParser recordParser, long samples) throws Exception {
            this.recordParser = recordParser;
            this.samples = samples;
            this.rectangle = rectangle;
        }

        public Builder filter(Rectangle rectangle) {
            this.rectangle = rectangle;
            return this;
        }

        public FilesSamping build() {
            return new FilesSamping(this);
        }

    }

    public void exportSamplesToFile(FileOutput fileOutput) throws Exception {

        DateFormat dateFormat = new SimpleDateFormat(recordParser.getDateFormat());

        Datasource ds = recordParser.getDatasource().cloneDatasource();
        RecordParser rp = recordParser.cloneRecordParser(ds);

        long numberOfRecords = 0;

        while (recordParser.hasNextRecord()) {

            Record record = recordParser.nextRecord();
            String lineMetaData = record.getMetadata();

            try {

                double longitude = Double.parseDouble(recordParser.getLongitude(record));
                double latitude = Double.parseDouble(recordParser.getLatitude(record));
                Date d = dateFormat.parse(recordParser.getDate(record));

                //filtering
                if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {
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

            } catch (NumberFormatException | ParseException | ArrayIndexOutOfBoundsException e) {

                if ((e instanceof NumberFormatException) || (e instanceof ParseException)) {
                    logger.warn("Spatial information of record can not be parsed {} \nLine {}", e, record.getMetadata());
                } else {
                    logger.warn("Record is incorrect {} \nLine {}", e, record.getMetadata());
                }

            }

        }

        if(samples>=numberOfRecords){
            throw new Exception("Samples can not be more than the existing number of records");
        }

        Random rand = new Random();
        Set<Long> randomNumbers = new HashSet<>();
        boolean i = true;
        while(i){
            long generatedLong = 1 + (long) (Math.random() * (numberOfRecords - 1));

            if(!randomNumbers.contains(generatedLong)){
                randomNumbers.add(generatedLong);

                if(randomNumbers.size() == samples){
                    i = false;
                }
            }
        }
        numberOfRecords = 0;

        while (rp.hasNextRecord()) {

            Record record = rp.nextRecord();
            String lineMetaData = record.getMetadata();

            try {

                double longitude = Double.parseDouble(rp.getLongitude(record));
                double latitude = Double.parseDouble(rp.getLatitude(record));
                Date d = dateFormat.parse(rp.getDate(record));

                //filtering
                if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {
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

                if(randomNumbers.contains(numberOfRecords)){
                    fileOutput.out(rp.toCsv(record),"samples.csv");
                    logger.debug("HERE {}",rp.toCsv(record));

                }

            } catch (NumberFormatException | ParseException | ArrayIndexOutOfBoundsException e) {

                if ((e instanceof NumberFormatException) || (e instanceof ParseException)) {
                    logger.warn("Spatial information of record can not be parsed {} \nLine {}", e, record.getMetadata());
                } else {
                    logger.warn("Record is incorrect {} \nLine {}", e, record.getMetadata());
                }

            }

        }

        fileOutput.close();
    }
}
