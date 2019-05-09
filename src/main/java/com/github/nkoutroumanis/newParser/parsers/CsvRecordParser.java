package com.github.nkoutroumanis.newParser.parsers;

import com.github.nkoutroumanis.AppConfig;
import com.github.nkoutroumanis.datasources.Datasource;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

import static com.github.nkoutroumanis.kafkaToMongoDB.Consts.*;

public class CsvRecordParser extends RecordParser {

    private static final Logger logger = LoggerFactory.getLogger(CsvRecordParser.class);

    private final int longitudeFieldId = AppConfig.getInstance().getConfig().getInt(inputLongitudeFieldIdSetting);
    private final int latitudeFieldId = AppConfig.getInstance().getConfig().getInt(inputLatitudeFieldIdSetting);
    private final int vehicleFieldId = AppConfig.getInstance().getConfig().getInt(inputVehicleFieldIdSetting);
    private final int dateFieldId = AppConfig.getInstance().getConfig().getInt(inputDateFieldIdSetting);

    private final String separator;
    private String[] headers;

    public CsvRecordParser(String separator, String header, Datasource source) {
        super(source);
        this.separator = separator;
        this.headers = header.split(separator);
    }

    @Override
    public Record nextRecord() throws ParseException {
        String[] lineWithMeta = source.nextLine();
        String[] fieldValues = lineWithMeta[0].split(this.separator);
        if (fieldValues.length != headers.length) {
            logger.error("Line has {} fields but {} fields are expected!\nLine: {}", fieldValues.length, headers.length, lineWithMeta[0]);
            throw new ParseException("Wrong input!", 0);
        }
        return new Record(fieldValues, lineWithMeta[1], headers);
    }

    public Document toDocument(Record record){

    }
}
