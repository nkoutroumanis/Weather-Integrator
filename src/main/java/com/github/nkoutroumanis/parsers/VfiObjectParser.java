package com.github.nkoutroumanis.parsers;

import com.github.nkoutroumanis.datasources.Datasource;
import com.github.nkoutroumanis.parsers.util.VfiMapPoint;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class VfiObjectParser extends RecordParser {

    private static final Logger logger = LoggerFactory.getLogger(VfiObjectParser.class);
    private Gson gson;
    private String[] headers;
    private DateFormat dateFormat;

    public VfiObjectParser(Datasource source) {
        super(source, VfiMapPoint.dateFormatStr);
        this.gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
        this.headers = VfiMapPoint.header.split(";");
        this.dateFormat = new SimpleDateFormat(VfiMapPoint.dateFormatStr);
    }

    @Override
    public Record nextRecord() throws ParseException {
        VfiMapPoint p = gson.fromJson(lineWithMeta[0].substring(2), VfiMapPoint.class);
        return new Record(p.getValuesInCsvOrder(), lineWithMeta[1], this.headers);
    }

    @Override
    public RecordParser cloneRecordParser(Datasource datasource) {
        return null;
    }

    @Override
    public String getLatitude(Record record) {
        return record.getFieldValues().get(VfiMapPoint.latitudeFieldId).toString();
    }

    @Override
    public String getLongitude(Record record) {
        return record.getFieldValues().get(VfiMapPoint.longitudeFieldId).toString();
    }

    @Override
    public String getDate(Record record) {
        Date d = new Date();
        d.setTime((Long)record.getFieldValues().get(VfiMapPoint.dateFieldId));
        return dateFormat.format(d);
    }
}
