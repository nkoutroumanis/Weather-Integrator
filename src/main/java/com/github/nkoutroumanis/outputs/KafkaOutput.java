package com.github.nkoutroumanis.outputs;

import com.github.nkoutroumanis.parsers.Record;
import com.github.nkoutroumanis.parsers.RecordParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class KafkaOutput implements Output {

    private final KafkaProducer<String, String> producer;

    private final RecordParser recordParser;

    private final String propertiesFile;
    private final String topicName;

    private KafkaOutput(RecordParser recordParser, String propertiesFile, String topicName) throws IOException {

        this.recordParser = recordParser;
        this.propertiesFile = propertiesFile;
        this.topicName = topicName;

        Properties props = new Properties();
        props.load(new FileInputStream(propertiesFile));

        producer = new KafkaProducer<>(props);
    }

    public static KafkaOutput newKafkaOutput(RecordParser recordParser, String propertiesFile, String topicName) throws IOException {
        return new KafkaOutput(recordParser, propertiesFile, topicName);
    }

    @Override
    public void out(Record record) {
        producer.send(new ProducerRecord<String, String>(topicName, recordParser.toCsv(record)));
    }

    @Override
    public void close() {
        producer.close();
    }
}
