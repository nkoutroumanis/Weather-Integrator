package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.Rectangle;
import com.github.nkoutroumanis.outputs.FileOutput;
import com.github.nkoutroumanis.outputs.KafkaOutput;
import com.github.nkoutroumanis.outputs.Output;
import com.github.nkoutroumanis.parsers.Record;
import com.github.nkoutroumanis.parsers.RecordParser;
import org.apache.spark.SparkContext;
import org.dia.core.SciSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

public final class WeatherIntegrator {

    private static final Logger logger = LoggerFactory.getLogger(WeatherIntegrator.class);

    private final RecordParser recordParser;
    private final WeatherDataObtainer wdo;

    private final boolean removeLastValueFromRecords;

    private final Rectangle rectangle;// = Rectangle.newRectangle(-180, -90, 180, 90);

    public static class Builder {

        private final RecordParser recordParser;

        private final String gribFilesFolderPath;
        private final List<String> variables;

        private String gribFilesExtension = ".grb2";
        private int lruCacheMaxEntries = 4;
        private boolean useIndex = false;

        private Rectangle rectangle = null;
        private boolean removeLastValueFromRecords = false;

        public Builder(RecordParser recordParser, String gribFilesFolderPath, List<String> variables) throws Exception {

            this.recordParser = recordParser;
            this.gribFilesFolderPath = gribFilesFolderPath;
            this.variables = variables;
        }

        public Builder gribFilesExtension(String gribFilesExtension) {
            this.gribFilesExtension = gribFilesExtension;
            return this;
        }

        public Builder filter(Rectangle rectangle) {
            this.rectangle = rectangle;
            return this;
        }

        public Builder removeLastValueFromRecords() {
            this.removeLastValueFromRecords = true;
            return this;
        }

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
        wdo = WeatherDataObtainer.newWeatherDataObtainer(builder.gribFilesFolderPath, builder.gribFilesExtension, builder.lruCacheMaxEntries, builder.useIndex, builder.variables);

        rectangle = builder.rectangle;
        removeLastValueFromRecords = builder.removeLastValueFromRecords;
    }

    public void integrateAndOutputToKafkaTopic(KafkaOutput kafkaOutput) throws Exception {
        integrate(kafkaOutput, (r) -> {
            return recordParser.toCsv(r, ";");
        });
    }

    public void integrateAndOutputToDirectory(FileOutput fileOutput) throws Exception {
        integrate(fileOutput, (r) -> {
            return recordParser.toCsv(r, ";");
        });

    }

    private void integrate(Output output, Function<Record, String> function) throws Exception {

        start = System.currentTimeMillis();

        DateFormat dateFormat = new SimpleDateFormat(recordParser.getDateFormat());

        long window = 0;
        long startTimeWindow = System.currentTimeMillis();

        while (recordParser.hasNextRecord()) {

            Record record = recordParser.nextRecord();

            try {

                double longitude = Double.parseDouble(recordParser.getLongitude(record));
                double latitude = Double.parseDouble(recordParser.getLatitude(record));
                Date d = dateFormat.parse(recordParser.getDate(record));


                if (rectangle != null) {
                    //filtering
                    if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {
                        logger.warn("Spatial information of record out of range \nLine{}", record.getMetadata());
                        continue;
                    }
                }

                if (removeLastValueFromRecords) {
                    //if dataset finishes with ;
                    record.deleteLastFieldValue();
                    //else sb.append(lineWithMeta);
                }


                List<Object> values = wdo.obtainAttributes(longitude, latitude, d);
                record.addFieldValues(values);

                numberofRecords++;

                if(numberofRecords%WeatherIntegratorJob.INFOEVERYN == 0){
                    logger.info("CHR {}", ((double) hits / numberofRecords));
                    logger.info("Overall Throughtput {}", ((double) WeatherIntegrator.numberofRecords / ((System.currentTimeMillis() - start) / 1000)));
                    logger.info("Window Throughtput {}", ((double) WeatherIntegratorJob.INFOEVERYN/((System.currentTimeMillis() - startTimeWindow) / 1000)));
                    logger.info("Opened {}", (numberofRecords - hits) - window);
                    window = numberofRecords - hits;
                    startTimeWindow = System.currentTimeMillis();
                }



                output.out(function.apply(record), record.getMetadata());

            } catch (NumberFormatException | ParseException e) {
                logger.warn("Spatio-temporal information of record can not be parsed {} \nLine {}", e, record.getMetadata());
            } catch (ArrayIndexOutOfBoundsException e) {
                logger.warn("Record is incorrect {} \nLine {}", e, record.getMetadata());
            }

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
