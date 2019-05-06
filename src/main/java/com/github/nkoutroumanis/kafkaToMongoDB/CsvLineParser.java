package com.github.nkoutroumanis.kafkaToMongoDB;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Arrays;

import static com.github.nkoutroumanis.kafkaToMongoDB.Consts.*;

public class CsvLineParser implements LineParser {

    private static final Logger logger = LoggerFactory.getLogger(CsvLineParser.class);

    private String separator;
    private String[] headers;
    private int vehicleFieldId;
    private int longitudeFieldId;
    private int latitudeFieldId;
    private int dateFieldId;

    public CsvLineParser(String separator, String header, int vehicleFieldId, int longitudeFieldId, int latitudeFieldId, int dateFieldId) {
        this.separator = separator;
        this.headers = header.split(separator);
        this.headers[vehicleFieldId] = vehicleFieldName;
        this.headers[dateFieldId] = dateFieldName;
        this.vehicleFieldId = vehicleFieldId;
        this.longitudeFieldId = longitudeFieldId;
        this.latitudeFieldId = latitudeFieldId;
        this.dateFieldId = dateFieldId;
    }

    @Override
    public Document parseLine(String line) throws ParseException {
        String[] fieldValues = line.split(this.separator);
        if (fieldValues.length != headers.length) {
            logger.error("Line has {} fields but {} fields are expected!\nLine: {}", fieldValues.length, headers.length, line);
            throw new ParseException("Wrong input!", 0);
        }
        Document result = new Document();
        for (int i = 0; i < fieldValues.length; i++) {
            if ((i != longitudeFieldId) && (i == latitudeFieldId)) {
                result.append(headers[i], fieldValues[i]);
            }
        }
        Document embeddedDoc = Consts.getPointDocument().append(
                coordinatesFieldName, Arrays.asList(fieldValues[longitudeFieldId], fieldValues[latitudeFieldId])
        );
        result.append(locationFieldName, embeddedDoc);
        return result;
    }
}
