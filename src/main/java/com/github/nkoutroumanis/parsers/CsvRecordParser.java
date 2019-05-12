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

    public CsvRecordParser(Datasource source, String separator, String headers, int vehicleFieldId, int longitudeFieldId, int latitudeFieldId, int dateFieldId, String dateFormat) {
        super(source);
        this.separator = separator;
        this.headers = headers.split(separator);
        this.vehicleFieldId = vehicleFieldId;
        this.longitudeFieldId = longitudeFieldId;
        this.latitudeFieldId = latitudeFieldId;
        this.dateFieldId = dateFieldId;
        this.dateFormat = dateFormat;
    }

    public CsvRecordParser(Datasource source, String separator, int longitudeFieldId, int latitudeFieldId, int dateFieldId, String dateFormat) {
        super(source);
        this.separator = separator;
        this.headers = null;
        this.vehicleFieldId = -1;
        this.longitudeFieldId = longitudeFieldId;
        this.latitudeFieldId = latitudeFieldId;
        this.dateFieldId = dateFieldId;
        this.dateFormat = dateFormat;
    }

    public CsvRecordParser(Datasource source, String separator, String headers) {
        super(source);
        this.separator = separator;
        this.headers = headers.split(separator);
        this.vehicleFieldId = -1;
        this.longitudeFieldId = -1;
        this.latitudeFieldId = -1;
        this.dateFieldId = -1;
        this.dateFormat = null;
    }

    @Override
    public Record nextRecord() throws ParseException {
        String[] fieldValues = lineWithMeta[0].split(this.separator, -1);
        if ((headers != null) && (fieldValues.length != headers.length)) {
            logger.error("Line has {} fields but {} fields are expected!\nLine: {}", fieldValues.length, headers.length, lineWithMeta[0]);
            throw new ParseException("Wrong input!", 0);
        }
        return new Record(fieldValues, lineWithMeta[1], headers);
    }

    @Override
    public Document toDocument(Record record) {
        //String[] fieldNames = record.getFieldNames();
        //String[] fieldValues = record.getFieldValues();

        if ((record.getFieldNames() == null) || (record.getFieldNames().size() != record.getFieldValues().size())) {
            logger.error("Field names is wrong!");
            return null;
        }

        Document result = new Document();
        for (int i = 0; i < record.getFieldValues().size(); i++) {
            if (i == vehicleFieldId) {
                result.append(vehicleFieldName, record.getFieldValues().get(i));
            }
            else if (i == dateFieldId) {
                result.append(dateFieldName, record.getFieldValues().get(i));
            }
            else if ((i != longitudeFieldId) && (i == latitudeFieldId)) {
                result.append(record.getFieldNames().get(i), record.getFieldValues().get(i));
            }
        }
        Document embeddedDoc = Consts.getPointDocument().append(
                coordinatesFieldName, Arrays.asList(record.getFieldValues().get(longitudeFieldId), record.getFieldValues().get(latitudeFieldId))
        );
        result.append(locationFieldName, embeddedDoc);
        return result;
    }

    @Override
    public String toCsv(Record record){

        StringBuilder sb = new StringBuilder();

        sb.append(record.getFieldValues().get(0));
        for(int i = 1; i< record.getFieldValues().size();i++){
            sb.append(separator);
            sb.append(record.getFieldValues().get(i));
        }

        return sb.toString();
    }

    @Override
    public String getLatitude(Record record) {
        return record.getFieldValues().get(latitudeFieldId-1);
    }

    @Override
    public String getLongitude(Record record) {
        return record.getFieldValues().get(longitudeFieldId-1);
    }

    @Override
    public String getDate(Record record) {
        return record.getFieldValues().get(dateFieldId-1);
    }

    @Override
    public String getDateFormat() {
        return this.dateFormat;
    }

    @Override
    public String getVehicle(Record record) {
        return record.getFieldValues().get(vehicleFieldId-1);
    }
}
