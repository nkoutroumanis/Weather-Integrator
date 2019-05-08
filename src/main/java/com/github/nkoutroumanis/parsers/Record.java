package com.github.nkoutroumanis.parsers;

import com.github.nkoutroumanis.AppConfig;
import com.github.nkoutroumanis.kafkaToMongoDB.Consts;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.github.nkoutroumanis.kafkaToMongoDB.Consts.*;

public class Record {

    private static final Logger logger = LoggerFactory.getLogger(Record.class);

    private static int longitudeFieldId = AppConfig.getInstance().getConfig().getInt(inputLongitudeFieldIdSetting);
    private static int latitudeFieldId = AppConfig.getInstance().getConfig().getInt(inputLatitudeFieldIdSetting);
    private static int vehicleFieldId = AppConfig.getInstance().getConfig().getInt(inputVehicleFieldIdSetting);
    private static int dateFieldId = AppConfig.getInstance().getConfig().getInt(inputDateFieldIdSetting);

    private String[] fieldValues;
    private String[] fieldNames;
    private String metadata;

    public Record(String[] fieldValues, String metadata) {
        this.fieldValues = fieldValues;
        this.metadata = metadata;
    }

    public Record(String[] fieldValues, String metadata, String[] fieldNames) {
        this(fieldValues, metadata);
        this.fieldNames = fieldNames;
    }

    public Document toDocument() {
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

    public String getMetadata() {
        return metadata;
    }
}
