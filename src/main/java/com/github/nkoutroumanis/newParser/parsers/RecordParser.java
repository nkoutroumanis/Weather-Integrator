package com.github.nkoutroumanis.newParser.parsers;

import com.github.nkoutroumanis.datasources.Datasource;

import java.io.IOException;
import java.text.ParseException;

public abstract class RecordParser {

    protected Datasource source;

    public RecordParser(Datasource source) {
        this.source = source;
    }

    public abstract Record nextRecord() throws ParseException;

    public boolean hasNextRecord() throws IOException {
        return this.source.hasNextLine();
    }
}
