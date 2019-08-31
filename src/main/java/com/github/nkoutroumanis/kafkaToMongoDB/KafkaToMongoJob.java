package com.github.nkoutroumanis.kafkaToMongoDB;

import com.github.nkoutroumanis.AppConfig;
import com.github.nkoutroumanis.datasources.Datasource;
import com.github.nkoutroumanis.datasources.KafkaDatasource;
import com.github.nkoutroumanis.outputs.MongoOutput;
import com.github.nkoutroumanis.outputs.Output;
import com.github.nkoutroumanis.parsers.CsvRecordParser;
import com.github.nkoutroumanis.parsers.Record;
import com.github.nkoutroumanis.parsers.RecordParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

import static com.github.nkoutroumanis.kafkaToMongoDB.Consts.*;

public class KafkaToMongoJob {

    private static final Logger logger = LoggerFactory.getLogger(KafkaToMongoJob.class);

    private static final Config config = ConfigFactory.load();

    private static Datasource getDatasource() throws IOException {
        String inputType = config.getString(inputTypeSetting);
        if (inputType.equals(streamType)) {
            logger.info("Using input type {}", streamType);
            return KafkaDatasource.newKafkaDatasource(
                    config.getString(inputKafkaPropsFileSetting),
                    config.getString(inputTopicNameSetting),
                    config.getLong(inputKafkaPollingSetting));
        } else {
            logger.error("Input type {} is not implemented", inputType);
            throw new UnsupportedOperationException();
        }
    }

    private static RecordParser getRecordParser() throws IOException {
        Datasource source = getDatasource();
        String inputFormat = config.getString(inputFormatSetting);
        if (inputFormat.equals(csvFormat)) {
            logger.info("Using input format {}", csvFormat);
            return new CsvRecordParser(
                    source,
                    config.getString(inputCsvSeparatorSetting),
                    config.getString(inputCsvHeaderSetting),
                    config.getInt(inputVehicleFieldIdSetting),
                    config.getInt(inputLongitudeFieldIdSetting),
                    config.getInt(inputLatitudeFieldIdSetting),
                    config.getInt(inputDateFieldIdSetting),
                    ""
            );
        } else {
            logger.error("Input format parser {} is not implemented", inputFormat);
            throw new UnsupportedOperationException();
        }
    }

    private static Output getOutput() {
        String outputType = config.getString(outputTypeSetting);
        if (outputType.equals(mongoType)) {
            logger.info("Using output type {}", mongoType);
            if (config.getBoolean(outputMongoSslEnabledSetting)) {
                logger.error("SSL encryption to MongoDB is not currently supported");
                throw new UnsupportedOperationException();
            }
            else {
                return new MongoOutput(
                        config.getString(outputMongoHostSetting),
                        config.getInt(outputMongoPortSetting),
                        config.getString(outputMongoDbSetting),
                        config.getString(outputMongoUserSetting),
                        config.getString(outputMongoPasswordSetting),
                        config.getString(outputMongoCollectionSetting),
                        config.getInt(outputMongoBatchSizeSetting)
                );
            }
        } else {
            logger.error("Output type {} is not implemented", outputType);
            throw new UnsupportedOperationException();
        }
    }

    public static void main(String[] args) throws Exception {
        RecordParser recordParser = getRecordParser();
        try (Output output = getOutput()) {

            Record record;
            Document doc;
            logger.info("Started filling mongo db");
            long reportCount = config.getLong(reportingNumberOfLinesSetting);
            long recordCount = 0;
            long startTime = System.currentTimeMillis();
            long totalElapsedTime, stepElapsedTime, curTime, prevTime = startTime;
            while (recordParser.hasNextRecord()) {
                record = recordParser.nextRecord();

                Config config = recordParser.toConfig(record);

                config = config.withoutPath(Consts.locationFieldName);
                config = config.withValue(Consts.locationFieldName + ".type", ConfigValueFactory.fromAnyRef("Point"));
                config = config.withValue(Consts.locationFieldName + ".coordinates", ConfigValueFactory.fromAnyRef(Arrays.asList(10,20)));

                config = config.withoutPath("VEHICLE_ID");
                config = config.withValue(Consts.vehicleFieldName, ConfigValueFactory.fromAnyRef(Consts.vehicleFieldName));

                config = config.withValue(Consts.dateFieldName, config.getValue("TIMESTAMP"));
                config = config.withoutPath("TIMESTAMP");

                //doc = recordParser.toDocument(record);
                output.out(Document.parse(config.root().render(ConfigRenderOptions.concise())), record.getMetadata());
                if ((++recordCount % reportCount) == 0) {
                    curTime = System.currentTimeMillis();
                    totalElapsedTime = (curTime - startTime) / 1000;
                    stepElapsedTime = (curTime - prevTime) / 1000;
                    prevTime = curTime;
                    logger.info("Processed {} records.\nTotal throughput {} msg/sec. Step throughput {} msg/sec.", recordCount, recordCount / totalElapsedTime, reportCount / stepElapsedTime);
                }
                //if (recordCount == 1000000) break;
            }
            curTime = System.currentTimeMillis();
            totalElapsedTime = (curTime - startTime) / 1000;
            logger.info("Process completed successfully!");
            logger.info("Total throughput: {} msg/sec.", recordCount / totalElapsedTime);
            logger.info("Total time: {} seconds.", totalElapsedTime);
        }
    }
}
