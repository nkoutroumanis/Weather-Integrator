package com.github.nkoutroumanis.weatherIntegrator;

import com.github.nkoutroumanis.datasources.FileDatasource;
import com.github.nkoutroumanis.outputs.KafkaOutput;
import com.github.nkoutroumanis.parsers.CsvRecordParser;
import com.github.nkoutroumanis.parsers.Record;

import java.io.IOException;
import java.text.ParseException;

public class LoadDataOnKafkaTopic {

    public static void main(String args[]) throws IOException, ParseException {

        FileDatasource fileDatasource = FileDatasource.newFileDatasource("/home/user/vehicles/",".csv");
        CsvRecordParser csvRecordParser = new CsvRecordParser(fileDatasource,";");
        KafkaOutput kafkaOutput = KafkaOutput.newKafkaOutput("./producer.properties","vehiclesWithoutWeather");

        while(csvRecordParser.hasNextRecord()){

            Record record = csvRecordParser.nextRecord();
            record.deleteLastFieldValue();

            kafkaOutput.out(csvRecordParser.toCsv(record,";"),"");
        }
        kafkaOutput.close();
        fileDatasource.cloneDatasource();
    }
}
