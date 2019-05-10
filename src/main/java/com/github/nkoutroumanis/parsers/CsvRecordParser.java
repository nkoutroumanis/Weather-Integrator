package com.github.nkoutroumanis.parsers;

import com.github.nkoutroumanis.datasources.Datasource;
import com.github.nkoutroumanis.kafkaToMongoDB.Consts;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Arrays;

import static com.github.nkoutroumanis.kafkaToMongoDB.Consts.*;

public class CsvRecordParser extends RecordParser {

    private static final Logger logger = LoggerFactory.getLogger(CsvRecordParser.class);

    private final int longitudeFieldId;// = AppConfig.config.getInt(inputLongitudeFieldIdSetting);
    private final int latitudeFieldId;// = AppConfig.config.getInt(inputLatitudeFieldIdSetting);
    private final int vehicleFieldId;// = AppConfig.config.getInt(inputVehicleFieldIdSetting);
    private final int dateFieldId;// = AppConfig.config.getInt(inputDateFieldIdSetting);

    private final String dateFormat;

    private final String separator;
    private String[] headers;

//    public CsvRecordParser(String separator, String header, Datasource source) {
//        super(source);
//        this.separator = separator;
//        this.headers = header.split(separator);
//    }

    public CsvRecordParser(String separator, String header, Datasource source, int vehicleFieldId, int longitudeFieldId, int latitudeFieldId, int dateFieldId, String dateFormat) {
        super(source);
        this.separator = separator;
        this.headers = header.split(separator);
        this.vehicleFieldId = vehicleFieldId;
        this.longitudeFieldId = longitudeFieldId;
        this.latitudeFieldId = latitudeFieldId;
        this.dateFieldId = dateFieldId;
        this.dateFormat = dateFormat;
    }

    @Override
    public Record nextRecord() throws ParseException {
        String[] fieldValues = lineWithMeta[0].split(this.separator);
        if ((headers != null) && (fieldValues.length != headers.length)) {
            logger.error("Line has {} fields but {} fields are expected!\nLine: {}", fieldValues.length, headers.length, lineWithMeta[0]);
            throw new ParseException("Wrong input!", 0);
        }
        return new Record(fieldValues, lineWithMeta[1], headers);
    }

    @Override
    public Document toDocument(Record record) {
        String[] fieldNames = record.getFieldNames();
        String[] fieldValues = record.getFieldValues();
        if ((fieldNames == null) || (fieldNames.length != fieldValues.length)) {
            logger.error("Field names is wrong!");
            return null;
        }

        Document result = new Document();
        for (int i = 0; i < fieldValues.length; i++) {
            if (i == vehicleFieldId) {
                result.append(vehicleFieldName, fieldValues[i]);
            }
            else if (i == dateFieldId) {
                result.append(dateFieldName, fieldValues[i]);
            }
            else if ((i != longitudeFieldId) && (i == latitudeFieldId)) {
                result.append(fieldNames[i], fieldValues[i]);
            }
        }
        Document embeddedDoc = Consts.getPointDocument().append(
                coordinatesFieldName, Arrays.asList(fieldValues[longitudeFieldId], fieldValues[latitudeFieldId])
        );
        result.append(locationFieldName, embeddedDoc);
        return result;
    }

    @Override
    public String getLatitude(Record record) {
        return record.getFieldValues()[latitudeFieldId];
    }

    @Override
    public String getLongitude(Record record) {
        return record.getFieldValues()[longitudeFieldId];
    }

    @Override
    public String getDate(Record record) {
        return record.getFieldValues()[dateFieldId];
    }

    @Override
    public String getDateFormat() {
        return this.dateFormat;
    }

    @Override
    public String getVehicle(Record record) {
        return record.getFieldValues()[vehicleFieldId];
    }
}
