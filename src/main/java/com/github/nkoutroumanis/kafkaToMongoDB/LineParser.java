package com.github.nkoutroumanis.kafkaToMongoDB;

import org.bson.Document;

import java.text.ParseException;

public interface LineParser {
    Document parseLine(String line) throws ParseException;
}
