package gr.ds.unipi.wi;

import ch.qos.logback.classic.LoggerContext;
import gr.ds.unipi.stpin.Rectangle;
import gr.ds.unipi.stpin.outputs.FileOutput;
import gr.ds.unipi.stpin.outputs.KafkaOutput;
import gr.ds.unipi.stpin.outputs.Output;
import gr.ds.unipi.stpin.parsers.JsonRecordParser;
import gr.ds.unipi.stpin.parsers.Record;
import gr.ds.unipi.stpin.parsers.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class WeatherIntegrator {

    private static final Logger logger = LoggerFactory.getLogger(WeatherIntegrator.class);

    private final RecordParser recordParser;
    private final WeatherDataObtainer wdo;
    private final List<String> variables;

    private final boolean removeLastValueFromRecords;
    private final Rectangle rectangle;

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

        variables = builder.variables;
        rectangle = builder.rectangle;
        removeLastValueFromRecords = builder.removeLastValueFromRecords;
    }

    public void integrate(KafkaOutput kafkaOutput) throws Exception {
        integrate(kafkaOutput, recordParser::toDefaultOutputFormat);
    }

    public void integrate(FileOutput fileOutput) throws Exception {
        integrate(fileOutput, recordParser::toDefaultOutputFormat);
    }

    private void integrate(Output output, Function<Record, String> function) throws Exception {

        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        System.out.println(lc.getProperty("reportEveryXlines"));
        long reportEveryXlines = Long.valueOf(lc.getProperty("reportEveryXlines"));
        if (lc.getProperty("reportEveryXlines") == null) {
            logger.error("reportEveryXlines property is not found in logback.xml");
        }

        start = System.currentTimeMillis();

        Function<Record, Date> dateFunction = recordParser.getDateFunction();

//        Function<Record, Date> dateFunction;
//        if(recordParser.getDateFormat().equals("unixTimestampSec")){
//            dateFunction = RecordParser.dateFunctionUnixTimestampSec(recordParser);
//        }
//        else if(recordParser.getDateFormat().equals("unixTimestampMillis")){
//            dateFunction = RecordParser.dateFunctionUnixTimestampMillis(recordParser);
//        }
//        else if(recordParser.getDateFormat().equals("unixTimestampDecimals")){
//            dateFunction = RecordParser.dateFunctionUnixTimestampDecimals(recordParser);
//        }
//        else{
//            recordParser.setSimpleDateFormat();
//            dateFunction = RecordParser.dateFunctionDateFormatPattern(recordParser);
//        }

        long window = 0;
        long startTimeWindow = System.currentTimeMillis();

        while (recordParser.hasNextRecord()) {

            Record record = recordParser.nextRecord();

            try {

                double longitude = Double.parseDouble(recordParser.getLongitude(record));
                double latitude = Double.parseDouble(recordParser.getLatitude(record));
                Date d = dateFunction.apply(record);

                if (d == null) {
                    continue;
                }

                if (rectangle != null) {
                    //filtering
                    if (((Double.compare(longitude, rectangle.getMaxx()) == 1) || (Double.compare(longitude, rectangle.getMinx()) == -1)) || ((Double.compare(latitude, rectangle.getMaxy()) == 1) || (Double.compare(latitude, rectangle.getMiny()) == -1))) {
                        logger.warn("Spatial information of record out of range \nLine{}", record.getMetadata());
                        continue;
                    }
                }

                if (removeLastValueFromRecords) {
                    //if dataset finishes with ; (for csv)
                    record.deleteLastFieldValue();
                    if (record.getFieldNames() != null) {
                        record.deleteLastFieldName();
                    }
                }

                List<Object> values = wdo.obtainAttributes(longitude, latitude, d);
                record.addFieldValues(values);

                if (recordParser instanceof JsonRecordParser) {
                    record.addFieldNames(variables.stream().map(u -> "weather_information." + u).collect(Collectors.toList()));
                }

                numberofRecords++;

                if (numberofRecords % reportEveryXlines == 0) {
                    logger.info("CHR {}", ((double) hits / numberofRecords));
                    logger.info("Overall Throughtput {}", ((double) WeatherIntegrator.numberofRecords / ((System.currentTimeMillis() - start) / 1000)));
                    logger.info("Window Throughtput {}", ((double) reportEveryXlines / ((System.currentTimeMillis() - startTimeWindow) / 1000)));
                    logger.info("Opened {}", (numberofRecords - hits) - window);
                    window = numberofRecords - hits;
                    startTimeWindow = System.currentTimeMillis();
                }

                output.out(function.apply(record), record.getMetadata());

            } catch (NumberFormatException e) {
                logger.warn("Spatial information of record can not be parsed {} \nLine {}", e, record.getMetadata());
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
