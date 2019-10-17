package gr.ds.unipi.stpin.parsers;

import gr.ds.unipi.stpin.datasources.Datasource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;

public class CsvRecordParser extends RecordParser {

    private static final Logger logger = LoggerFactory.getLogger(CsvRecordParser.class);

    private final int longitudeFieldId;// = AppConfig.config.getInt(inputLongitudeFieldIdSetting);
    private final int latitudeFieldId;// = AppConfig.config.getInt(inputLatitudeFieldIdSetting);
    private final int vehicleFieldId;// = AppConfig.config.getInt(inputVehicleFieldIdSetting);
    private final int dateFieldId;// = AppConfig.config.getInt(inputDateFieldIdSetting);

    private final String separator;
    private String[] headers;

//    public CsvRecordParser(String separator, String header, Datasource source) {
//        super(source);
//        this.separator = separator;
//        this.headers = header.split(separator);
//    }

    public CsvRecordParser(Datasource source, String separator, String headers, int vehicleFieldId, int longitudeFieldId, int latitudeFieldId, int dateFieldId, String dateFormat) {
        super(source, dateFormat);
        this.separator = separator;
        this.headers = headers.split(separator);
        this.vehicleFieldId = vehicleFieldId;
        this.longitudeFieldId = longitudeFieldId;
        this.latitudeFieldId = latitudeFieldId;
        this.dateFieldId = dateFieldId;
    }

    public CsvRecordParser(Datasource source, String separator, int longitudeFieldId, int latitudeFieldId, int dateFieldId, String dateFormat) {
        super(source, dateFormat);
        this.separator = separator;
        this.headers = null;
        this.vehicleFieldId = -1;
        this.longitudeFieldId = longitudeFieldId;
        this.latitudeFieldId = latitudeFieldId;
        this.dateFieldId = dateFieldId;
    }

    public CsvRecordParser(Datasource source, String separator, String headers) {
        this(source, separator, headers, -1, -1, -1, -1, null);
    }

    public CsvRecordParser(Datasource source, String separator) {
        this(source, separator, -1, -1, -1, null);
    }

    public CsvRecordParser(Datasource source, String separator, int longitudeFieldId, int latitudeFieldId) {
        super(source, null);
        this.separator = separator;
        this.headers = null;
        this.vehicleFieldId = -1;
        this.longitudeFieldId = longitudeFieldId;
        this.latitudeFieldId = latitudeFieldId;
        this.dateFieldId = -1;

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

//    @Override
//    public Document toDocument(Record record) {
//        //String[] fieldNames = record.getFieldNames();
//        //String[] fieldValues = record.getFieldValues();
//
//        if ((record.getFieldNames() == null) || (record.getFieldNames().size() != record.getFieldValues().size())) {
//            logger.error("Field names is wrong!");
//            return null;
//        }
//
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
        return (String) record.getFieldValues().get(latitudeFieldId - 1);
    }

    @Override
    public String getLongitude(Record record) {
        return (String) record.getFieldValues().get(longitudeFieldId - 1);
    }

    @Override
    public String getDate(Record record) {
        return (String) record.getFieldValues().get(dateFieldId - 1);
    }

    @Override
    public String getVehicle(Record record) {
        return (String) record.getFieldValues().get(vehicleFieldId - 1);
    }

    @Override
    public RecordParser cloneRecordParser(Datasource source) {
        return new CsvRecordParser(source, separator, headers, vehicleFieldId, longitudeFieldId, latitudeFieldId, dateFieldId, dateFormat);
    }

    @Override
    public String getVehicleFieldName(Record record) {
        return record.getFieldNames().get(vehicleFieldId - 1);
    }

    @Override
    public String getLatitudeFieldName(Record record) {
        return record.getFieldNames().get(latitudeFieldId - 1);
    }

    @Override
    public String getLongitudeFieldName(Record record) {
        return record.getFieldNames().get(longitudeFieldId - 1);
    }

    @Override
    public String getDateFieldName(Record record) {
        return record.getFieldNames().get(dateFieldId - 1);
    }

    private CsvRecordParser(Datasource source, String separator, String[] headers, int vehicleFieldId, int longitudeFieldId, int latitudeFieldId, int dateFieldId, String dateFormat){
        super(source, dateFormat);
        this.separator = separator;
        this.headers = headers;
        this.vehicleFieldId = vehicleFieldId;
        this.longitudeFieldId = longitudeFieldId;
        this.latitudeFieldId = latitudeFieldId;
        this.dateFieldId = dateFieldId;
    }

    public CsvRecordParser(CsvRecordParser csvRecordParser){
        super(csvRecordParser.source, csvRecordParser.dateFormat);
        separator = csvRecordParser.separator;
        headers = csvRecordParser.headers;
        vehicleFieldId = csvRecordParser.vehicleFieldId;
        longitudeFieldId = csvRecordParser.longitudeFieldId;
        latitudeFieldId = csvRecordParser.latitudeFieldId;
        dateFieldId = csvRecordParser.dateFieldId;
    }
}
