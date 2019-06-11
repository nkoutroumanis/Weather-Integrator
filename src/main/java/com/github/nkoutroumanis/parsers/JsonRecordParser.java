package com.github.nkoutroumanis.parsers;

import com.github.nkoutroumanis.datasources.Datasource;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

public class JsonRecordParser extends RecordParser {

    private static final Logger logger = LoggerFactory.getLogger(JsonRecordParser.class);

    private final String longitudeFieldName;
    private final String latitudeFieldName;
    private final String vehicleFieldName;
    private final String dateFieldName;

    private final String dateFormat;

    public JsonRecordParser(Datasource source, String vehicleFieldName, String longitudeFieldName, String latitudeFieldName, String dateFieldName, String dateFormat) {
        super(source);

        this.vehicleFieldName = vehicleFieldName;
        this.longitudeFieldName = longitudeFieldName;
        this.latitudeFieldName = latitudeFieldName;
        this.dateFieldName = dateFieldName;
        this.dateFormat = dateFormat;
    }

    public JsonRecordParser(Datasource source, String longitudeFieldName, String latitudeFieldName, String dateFieldName, String dateFormat) {
        super(source);

        this.vehicleFieldName = "";
        this.longitudeFieldName = longitudeFieldName;
        this.latitudeFieldName = latitudeFieldName;
        this.dateFieldName = dateFieldName;
        this.dateFormat = dateFormat;
    }

    public JsonRecordParser(Datasource source) {
        this(source, "", "", "", "", null);
    }

    public JsonRecordParser(Datasource source, String longitudeFieldName, String latitudeFieldName) {
        super(source);

        this.vehicleFieldName = "";
        this.longitudeFieldName = longitudeFieldName;
        this.latitudeFieldName = latitudeFieldName;
        this.dateFieldName = "";
        this.dateFormat = null;

    }

    public JsonRecordParser(JsonRecordParser jsonRecordParser){
        super(jsonRecordParser.source);

        vehicleFieldName = jsonRecordParser.vehicleFieldName;
        longitudeFieldName = jsonRecordParser.longitudeFieldName;
        latitudeFieldName = jsonRecordParser.latitudeFieldName;
        dateFieldName = jsonRecordParser.dateFieldName;
        dateFormat = jsonRecordParser.dateFormat;
    }

    @Override
    public Record nextRecord() throws ParseException {
        String[] fieldValues = lineWithMeta[0].split(this.separator, -1);
        Gson gson = new Gson();
        gson.fromJson().toJson(,)

        return new Record(fieldValues, lineWithMeta[1], headers);
    }

    @Override
    public RecordParser cloneRecordParser(Datasource datasource) {
        return null;
    }
}
