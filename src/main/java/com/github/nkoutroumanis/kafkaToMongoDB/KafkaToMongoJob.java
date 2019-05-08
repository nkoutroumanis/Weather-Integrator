package com.github.nkoutroumanis.kafkaToMongoDB;

import com.github.nkoutroumanis.KafkaParser;
import com.github.nkoutroumanis.MongoOutput;
import com.github.nkoutroumanis.Output;
import com.github.nkoutroumanis.Parser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.text.ParseException;

import static com.github.nkoutroumanis.kafkaToMongoDB.Consts.*;

public class KafkaToMongoJob {

    private static final Logger logger = LoggerFactory.getLogger(KafkaToMongoJob.class);

    private static final Config config = ConfigFactory.load();

    private static Parser getInputParser() throws IOException {
        String inputType = config.getString(inputTypeSetting);
        if (inputType.equals(streamType)) {
            logger.info("Using input type {}", streamType);
            return KafkaParser.newKafkaParser(
                    config.getString(inputKafkaPropsFileSetting),
                    config.getString(inputTopicNameSetting),
                    config.getLong(inputKafkaPollingSetting));
        } else {
            logger.error("Input type {} is not implemented", inputType);
            throw new NotImplementedException();
        }
    }

    private static LineParser getLineParser() {
        String inputFormat = config.getString(inputFormatSetting);
        if (inputFormat.equals(csvFormat)) {
            logger.info("Using input format {}", csvFormat);
            return new CsvLineParser(
                    config.getString(inputCsvSeparatorSetting),
                    config.getString(inputCsvHeaderSetting),
                    config.getInt(inputVehicleFieldIdSetting),
                    config.getInt(inputLongitudeFieldIdSetting),
                    config.getInt(inputLatitudeFieldIdSetting),
                    config.getInt(inputDateFieldIdSetting));
        } else {
            logger.error("Input format parser {} is not implemented", inputFormat);
            throw new NotImplementedException();
        }
    }

    private static Output getOutput() {
        String outputType = config.getString(outputTypeSetting);
        if (outputType.equals(mongoType)) {
            logger.info("Using output type {}", mongoType);
            return new MongoOutput(
                    config.getString(outputMongoHostSetting),
                    config.getInt(outputMongoPortSetting),
                    config.getString(outputMongoDbSetting),
                    config.getString(outputMongoUserSetting),
                    config.getString(outputMongoPasswordSetting),
                    config.getString(outputMongoCollectionSetting),
                    config.getInt(outputMongoBatchSizeSetting)
            );
        } else {
            logger.error("Output type {} is not implemented", outputType);
            throw new NotImplementedException();
        }
    }

    public static void main(String[] args) throws IOException, ParseException {
        Parser inputParser = getInputParser();
        LineParser lineParser = getLineParser();
        Output output = getOutput();

        String[] line;
        Document doc;
        logger.info("Started filling mongo db");
        long reportCount = config.getLong(reportingNumberOfLinesSetting);
        long lineCount = 0;
        while (inputParser.hasNextLine()) {
            line = inputParser.nextLine();
            doc = lineParser.parseLine(line[0]);
            output.out(doc.toJson(), line[1]);
            if ((++lineCount % reportCount) == 0) {
                logger.info("Processed {} lines", lineCount);
            }
        }
        logger.info("Process completed successfully!");
    }
}
