package com.github.nkoutroumanis.kafkaToMongoDB;

import org.bson.Document;

public class Consts {
    public static final String inputTopicNameSetting = "input.options.kafkaTopicName";
    public static final String inputKafkaPropsFileSetting = "input.options.kafkaPropertiesFileName";
    public static final String inputKafkaPollingSetting = "input.options.kafka.poll.ms";
    public static final String inputTypeSetting = "input.type";
    public static final String inputFormatSetting = "input.format";
    public static final String inputCsvSeparatorSetting = "input.options.csvSeparator";
    public static final String inputCsvHeaderSetting = "input.options.csvHeader";
    public static final String inputDateFieldIdSetting = "input.dateFieldId";
    public static final String inputLongitudeFieldIdSetting = "input.longitudeFieldId";
    public static final String inputLatitudeFieldIdSetting = "input.latitudeFieldId";
    public static final String inputVehicleFieldIdSetting = "input.vehicleFieldId";

    public static final String outputTypeSetting = "output.type";
    public static final String outputMongoBatchSizeSetting = "output.options.mongoBatchSize";
    public static final String outputMongoHostSetting = "output.options.mongoHost";
    public static final String outputMongoPortSetting = "output.options.mongoPort";
    public static final String outputMongoDbSetting = "output.options.mongoDB";
    public static final String outputMongoCollectionSetting = "output.options.mongoCollection";
    public static final String outputMongoUserSetting = "output.options.mongoUser";
    public static final String outputMongoPasswordSetting = "output.options.mongoPass";
    public static final String outputMongoSslEnabledSetting = "output.options.mongoSslEnabled";

    public static final String reportingNumberOfLinesSetting = "reporting.everyNumberOfLines";

    public static final String csvFormat = "csv";
    public static final String mongoType = "mongo";
    public static final String streamType = "stream";

    public static final String vehicleFieldName = "vehicle";
    public static final String dateFieldName = "date";
    public static final String coordinatesFieldName = "coordinates";
    public static final String locationFieldName = "location";

    public static Document getPointDocument() {
        return new Document("type", "Point");
    }
}
