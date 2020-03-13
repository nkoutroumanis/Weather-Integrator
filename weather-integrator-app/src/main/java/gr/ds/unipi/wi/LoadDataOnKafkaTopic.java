package gr.ds.unipi.wi;

import gr.ds.unipi.stpin.datasources.FileDatasource;
import gr.ds.unipi.stpin.outputs.KafkaOutput;
import gr.ds.unipi.stpin.parsers.CsvRecordParser;
import gr.ds.unipi.stpin.parsers.Record;

import java.io.IOException;
import java.text.ParseException;

public class LoadDataOnKafkaTopic {

    public static void main(String args[]) throws IOException, ParseException {

        FileDatasource fileDatasource = FileDatasource.newFileDatasource("/home/user/vehicles/", ".csv");
        CsvRecordParser csvRecordParser = new CsvRecordParser(fileDatasource, ";");
        KafkaOutput kafkaOutput = KafkaOutput.newKafkaOutput("./producer.properties", "vehiclesWithoutWeather");

        while (csvRecordParser.hasNextRecord()) {
            Record record = csvRecordParser.nextRecord();
            record.deleteLastFieldValue();
            kafkaOutput.out(csvRecordParser.toCsv(record, ";"), "");
        }
        kafkaOutput.close();
        fileDatasource.cloneDatasource();
    }
}
