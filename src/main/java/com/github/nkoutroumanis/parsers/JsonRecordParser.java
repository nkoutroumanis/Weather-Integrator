package com.github.nkoutroumanis.parsers;

import com.github.nkoutroumanis.datasources.Datasource;
import com.google.gson.Gson;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.*;

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

        String json = lineWithMeta[0];

        Config config = ConfigFactory.parseString(json);
        Set<Map.Entry<String, ConfigValue>> jsonSet = config.entrySet();

       String[] headers = new String[jsonSet.size()];
       String[] fieldValues = new String[jsonSet.size()];

       int k = 0;

        for (Map.Entry<String, ConfigValue> entry: jsonSet) {
            headers[k] = entry.getKey();
            fieldValues[k] = entry.getValue().render();
            k++;
        }

        return new Record(fieldValues, lineWithMeta[1], headers);
    }

//    @Override
//    public String toJsonString(Record record) {
//
//        List<String> fieldNames = record.getFieldNames();
//        List<String> fieldValues = record.getFieldValues();
//
//        if ((record.getFieldNames() == null) || (record.getFieldNames().size() != record.getFieldValues().size())) {
//            logger.error("Field names is wrong!");
//            return null;
//        }
//
//        Map<String, Object> properties = new HashMap<>();
//        for(int i =0; i<fieldNames.size();i++){
//            properties.put(fieldNames.get(i), fieldValues.get(i));
//        }
//
//        Config config = ConfigFactory.parseMap(properties);
//        return config.root().render(ConfigRenderOptions.concise());
//        Document doc = Document.parse(config.root().render(ConfigRenderOptions.concise()));
//
//        return doc;
//        Document result = new Document();
//        for (int i = 0; i < record.getFieldValues().size(); i++) {
//            if (i == vehicleFieldId) {
//                result.append(vehicleFieldName, record.getFieldValues().get(i));
//            } else if (i == dateFieldId) {
//                result.append(dateFieldName, record.getFieldValues().get(i));
//            } else if ((i != longitudeFieldId) && (i != latitudeFieldId)) {
//                result.append(record.getFieldNames().get(i), record.getFieldValues().get(i));
//            }
//        }
//        double longitude = Double.parseDouble(record.getFieldValues().get(longitudeFieldId));
//        double latitude = Double.parseDouble(record.getFieldValues().get(latitudeFieldId));
//        Document embeddedDoc = Consts.getPointDocument().append(
//                coordinatesFieldName, Arrays.asList(longitude, latitude)
//        );
//        result.append(locationFieldName, embeddedDoc);
//        return result;

//    }

    @Override
    public String getLatitude(Record record) {

        List<String> fieldNames = record.getFieldNames();
        List<String> fieldValues = record.getFieldValues();


        for(int i=0;i<fieldNames.size();i++){
            if(fieldNames.get(i).equals(latitudeFieldName)){
                return fieldValues.get(i);
            }
        }

        return null;
    }

    @Override
    public String getLongitude(Record record) {
        List<String> fieldNames = record.getFieldNames();
        List<Object> fieldValues = record.getFieldValues();


        for(int i=0;i<fieldNames.size();i++){
            if(fieldNames.get(i).equals(longitudeFieldName)){
                return fieldValues.get(i);
            }
        }

        return null;
    }

    @Override
    public String getDate(Record record) {
        List<String> fieldNames = record.getFieldNames();
        List<Object> fieldValues = record.getFieldValues();


        for(int i=0;i<fieldNames.size();i++){
            if(fieldNames.get(i).equals(dateFieldName)){
                return fieldValues.get(i);
            }
        }

        return null;
    }

    @Override
    public String getVehicle(Record record) {
        List<String> fieldNames = record.getFieldNames();
        List<Object> fieldValues = record.getFieldValues();


        for(int i=0;i<fieldNames.size();i++){
            if(fieldNames.get(i).equals(vehicleFieldName)){
                return fieldValues.get(i);
            }
        }

        return null;
    }

    @Override
    public RecordParser cloneRecordParser(Datasource datasource) {
        return null;
    }
}
