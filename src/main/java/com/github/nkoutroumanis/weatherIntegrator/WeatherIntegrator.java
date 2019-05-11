package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.*;
import com.github.nkoutroumanis.datasources.KafkaDatasource;
import com.github.nkoutroumanis.kafkaToMongoDB.KafkaToMongoJob;
import com.github.nkoutroumanis.parsers.Record;
import com.github.nkoutroumanis.parsers.RecordParser;
import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.outputs.KafkaOutput;
import com.github.nkoutroumanis.outputs.Output;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public final class WeatherIntegrator {

    private static final Logger logger = LoggerFactory.getLogger(WeatherIntegrator.class);

    private final RecordParser recordParser;
    private final WeatherDataObtainer wdo;

    private final boolean checkLonLatRanges;
    private final boolean removeLastValueFromRecords;

//    private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
//    private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
//    private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
//
//    private final DateFormat dateFormat;
//
//    private final String separator;

    private final Rectangle rectangle = Rectangle.newRectangle(-180, -90, 180, 90);

    public static class Builder {

        private final RecordParser recordParser;

        private final String gribFilesFolderPath;

//        private final int numberOfColumnLongitude;//1 if the 1st column represents the longitude, 2 if the 2nd column...
//        private final int numberOfColumnLatitude;//1 if the 1st column represents the latitude, 2 if the 2nd column...
//        private final int numberOfColumnDate;//1 if the 1st column represents the date, 2 if the 2nd column...
//
//        private final DateFormat dateFormat;
        private final List<String> variables;

        private String gribFilesExtension = ".grb2";
        //private String separator = ";";
        private int lruCacheMaxEntries = 4;
        private boolean useIndex = false;

        private boolean checkLonLatRanges = false;
        private boolean removeLastValueFromRecords = false;

        public Builder(RecordParser recordParser, String gribFilesFolderPath, List<String> variables) throws Exception {

            this.recordParser = recordParser;
            this.gribFilesFolderPath = gribFilesFolderPath;
//            this.numberOfColumnLongitude = numberOfColumnLongitude;
//            this.numberOfColumnLatitude = numberOfColumnLatitude;
//            this.numberOfColumnDate = numberOfColumnDate;
//            this.dateFormat = new SimpleDateFormat(dateFormat);
            this.variables = variables;
        }

        public Builder gribFilesExtension(String gribFilesExtension) {
            this.gribFilesExtension = gribFilesExtension;
            return this;
        }

        public Builder checkLonLatRanges() {
            this.checkLonLatRanges = true;
            return this;
        }

        public Builder removeLastValueFromRecords() {
            this.removeLastValueFromRecords = true;
            return this;
        }

//        public Builder separator(String separator) {
//            this.separator = separator;
//            return this;
//        }

        public Builder lruCacheMaxEntries(int lruCacheMaxEntries) {
            this.lruCacheMaxEntries = lruCacheMaxEntries;
            return this;
        }

        public Builder useIndex() {
            this.useIndex = true;
            return this;
        }

        public WeatherIntegrator build() throws Exception {
            return new WeatherIntegrator(this);
        }

    }

    private WeatherIntegrator(WeatherIntegrator.Builder builder) throws Exception {

        recordParser = builder.recordParser;
//        numberOfColumnLongitude = builder.numberOfColumnLongitude;
//        numberOfColumnLatitude = builder.numberOfColumnLatitude;
//        numberOfColumnDate = builder.numberOfColumnDate;
//        dateFormat = builder.dateFormat;
//        separator = builder.separator;
        wdo = WeatherDataObtainer.newWeatherDataObtainer(builder.gribFilesFolderPath, builder.gribFilesExtension, builder.lruCacheMaxEntries, builder.useIndex, builder.variables);

        checkLonLatRanges = builder.checkLonLatRanges;
        removeLastValueFromRecords = builder.removeLastValueFromRecords;
    }

    public void integrateAndOutputToKafkaTopic(String propertiesFile, String topicName) throws IOException, ParseException {
        integrate(KafkaOutput.newKafkaOutput(recordParser, propertiesFile, topicName));
    }

    public void integrateAndOutputToDirectory(String directory, boolean deleteDirectoryIfExist) throws IOException, ParseException {
        integrate(FileOutput.newFileOutput(recordParser, directory, deleteDirectoryIfExist));
    }

    private void integrate(Output output) throws IOException, ParseException {

        start = System.currentTimeMillis();

        DateFormat dateFormat = new SimpleDateFormat(recordParser.getDateFormat());

        while (recordParser.hasNextRecord()) {

            Record record = recordParser.nextRecord();

            try{

                double longitude = Double.parseDouble(recordParser.getLongitude(record));
                double latitude = Double.parseDouble(recordParser.getLatitude(record));
                Date d = dateFormat.parse(recordParser.getDate(record));


                if(checkLonLatRanges){
                    //filtering
                    if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {
                        logger.warn("Spatial information of record out of range \nLine{}", record.getMetadata());
                        continue;
                    }
                }

                List<String> values = wdo.obtainAttributes(longitude, latitude, d);
                record.addFieldValues(values);


                if(removeLastValueFromRecords){
                    //if dataset finishes with ;
                    record.deleteLastFieldValue();
                    //else sb.append(lineWithMeta);
                }

                numberofRecords++;

                output.out(record);

            }
            catch (NumberFormatException | ParseException e){
                logger.warn("Spatio-temporal information of record can not be parsed {} \nLine {}", e, record.getMetadata());
                continue;
            }
            catch (ArrayIndexOutOfBoundsException e){
                logger.warn("Record is incorrect {} \nLine {}", e, record.getMetadata());
                continue;
            }


//            try {
//                String[] a = datasource.nextLine();
//
//                String line = a[0];
//                String[] separatedLine = line.split(separator);
//
//                if (Datasource.empty.test(separatedLine[numberOfColumnLongitude - 1]) || Datasource.empty.test(separatedLine[numberOfColumnLatitude - 1]) || Datasource.empty.test(separatedLine[numberOfColumnDate - 1])) {
//                    continue;
//                }
//
//                double longitude = Double.parseDouble(separatedLine[numberOfColumnLongitude - 1]);
//                double latitude = Double.parseDouble(separatedLine[numberOfColumnLatitude - 1]);
//                Date d = dateFormat.parse(separatedLine[numberOfColumnDate - 1]);
//
//                //filtering
//                if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {
//                    continue;
//                }
//
//
//                StringBuilder sb = new StringBuilder();
//
//                //if dataset finishes with ;
//                sb.append(line.substring(0, line.length() - 1));
//                //else sb.append(lineWithMeta);
//
//                numberofRecords++;
//
//                List<String> values = wdo.obtainAttributes(longitude, latitude, d);
//
//                values.forEach(s -> sb.append(separator + s));
//
//                output.out(sb.toString(), a[1]);
//            } catch (ArrayIndexOutOfBoundsException | NumberFormatException | ParseException e) {
//                continue;
//            }

        }

        elapsedTime = (System.currentTimeMillis() - start) / 1000;
        output.close();

        Runtime rt = Runtime.getRuntime();

        logger.info("Approximation of used Memory: {} MB", (rt.totalMemory() - rt.freeMemory()) / 1000000);
        logger.info("Elapsed Time: {} sec", elapsedTime);
        logger.info("Number Of Processed Records: {}", WeatherIntegrator.numberofRecords);
        logger.info("Number Of Hits: {}", WeatherIntegrator.hits);
        logger.info("CHR (Number Of Hits)/(Number Of Records): {}", ((double) hits / numberofRecords));
        logger.info("Throughput (records/sec): {}", ((double) WeatherIntegrator.numberofRecords / WeatherIntegrator.elapsedTime));

    }

    public static WeatherIntegrator.Builder newWeatherIntegrator(RecordParser recordParser, String gribFilesFolderPath, List<String> variables) throws Exception {
        return new WeatherIntegrator.Builder(recordParser, gribFilesFolderPath, variables);
    }

    private static long start;
    private static long elapsedTime;

    public static long hits = 0;
    private static long numberofRecords = 0;

}
