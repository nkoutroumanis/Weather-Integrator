package gr.ds.unipi.stpin.parsers;

import gr.ds.unipi.stpin.datasources.Datasource;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class RecordParser {

    private static final Logger logger = LoggerFactory.getLogger(RecordParser.class);

    protected Datasource source;
    protected String[] lineWithMeta;
    protected final String dateFormat;

    public RecordParser(Datasource source, String dateFormat) {
        this.source = source;
        this.dateFormat = dateFormat;
    }

    public abstract Record nextRecord() throws ParseException;

    public boolean hasNextRecord() throws IOException {
        lineWithMeta = new String[]{"", ""};
        while (((lineWithMeta[0].length() == 0) || !isLineValid()) && this.source.hasNextLine()) {
            lineWithMeta = this.source.nextLine();
            lineWithMeta[0] = lineWithMeta[0].trim();
        }
        return lineWithMeta[0].length() != 0;
    }

    protected boolean isLineValid() {
        return true;
    }


    public String toJsonString(Record record) {

        return toConfig(record).root().render(ConfigRenderOptions.concise());

    }

    public Config toConfig(Record record) {

        List<String> fieldNames = record.getFieldNames();
        List<Object> fieldValues = record.getFieldValues();

        if ((record.getFieldNames() == null) || (record.getFieldNames().size() != record.getFieldValues().size())) {
            logger.error("Field names is wrong!");
            return null;
        }

        Map<String, Object> properties = new HashMap<>();
        for(int i =0; i<fieldNames.size();i++){
            properties.put(fieldNames.get(i), fieldValues.get(i));
        }

        return ConfigFactory.parseMap(properties);

    }

    public String toCsv(Record record, String separator) {

        StringBuilder sb = new StringBuilder();

        sb.append(record.getFieldValues().get(0));
        for (int i = 1; i < record.getFieldValues().size(); i++) {
            sb.append(separator);
            sb.append(record.getFieldValues().get(i));
        }

        return sb.toString();
    }

    public String getLatitude(Record record) {
        throw new UnsupportedOperationException();
    }

    public String getLongitude(Record record) {
        throw new UnsupportedOperationException();
    }

    public String getDate(Record record) {
        throw new UnsupportedOperationException();
    }

    public String getDateFormat() {
        return this.dateFormat;
    }

    public String getVehicle(Record record) {
        throw new UnsupportedOperationException();
    }

    public Datasource getDatasource() {
        return source;
    }

    public abstract RecordParser cloneRecordParser(Datasource datasource);
}
