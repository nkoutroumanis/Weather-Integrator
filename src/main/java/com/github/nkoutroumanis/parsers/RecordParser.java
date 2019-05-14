package com.github.nkoutroumanis.parsers;

import com.github.nkoutroumanis.datasources.Datasource;
import org.bson.Document;

import java.io.IOException;
import java.text.ParseException;

public abstract class RecordParser {

    protected Datasource source;
    protected String[] lineWithMeta;

    public RecordParser(Datasource source) {
        this.source = source;
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


    public Document toDocument(Record record) {
        throw new UnsupportedOperationException();
    }

    public String toCsv(Record record) {
        throw new UnsupportedOperationException();
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
        throw new UnsupportedOperationException();
    }

    public String getVehicle(Record record) {
        throw new UnsupportedOperationException();
    }

    public Datasource getDatasource() {
        return source;
    }
}
