package com.github.nkoutroumanis.parsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class Record {

    private static final Logger logger = LoggerFactory.getLogger(Record.class);

//    private static int longitudeFieldId = AppConfig.getInstance().getConfig().getInt(inputLongitudeFieldIdSetting);
//    private static int latitudeFieldId = AppConfig.getInstance().getConfig().getInt(inputLatitudeFieldIdSetting);
//    private static int vehicleFieldId = AppConfig.getInstance().getConfig().getInt(inputVehicleFieldIdSetting);
//    private static int dateFieldId = AppConfig.getInstance().getConfig().getInt(inputDateFieldIdSetting);

    private List<String> fieldValues;
    private List<String> fieldNames;
    private String metadata;

    public Record(String[] fieldValues, String metadata) {
        this.fieldValues = Arrays.asList(fieldValues);
        this.metadata = metadata;
    }

    public Record(String[] fieldValues, String metadata, String[] fieldNames) {
        this(fieldValues, metadata);
        this.fieldNames = Arrays.asList(fieldNames);
    }

    public String getMetadata() {
        return metadata;
    }

    public String[] getFieldNames() {
        return fieldNames.toArray(new String[0]);
    }

    public String[] getFieldValues() {
        return fieldValues.toArray(new String[0]);
    }

    public void addFieldValues(List<String> newFieldValues) {
        this.fieldValues.addAll(newFieldValues);
    }

    public void addFieldNames(List<String> newFieldNames) {
        this.fieldNames.addAll(newFieldNames);
    }

    public void addFieldValue(String newFieldValue) {
        this.fieldValues.add(newFieldValue);
    }

    public void addFieldName(String newFieldName) {
        this.fieldNames.add(newFieldName);
    }
}
