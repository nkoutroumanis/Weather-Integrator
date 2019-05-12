package com.github.nkoutroumanis.outputs;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class KafkaOutput implements Output {

    private final KafkaProducer<String, String> producer;

    private final String propertiesFile;
    private final String topicName;

    private KafkaOutput(String propertiesFile, String topicName) throws IOException {

        this.propertiesFile = propertiesFile;
        this.topicName = topicName;

        Properties props = new Properties();
        props.load(new FileInputStream(propertiesFile));

        producer = new KafkaProducer<>(props);
    }

    public static KafkaOutput newKafkaOutput(String propertiesFile, String topicName) throws IOException {
        return new KafkaOutput(propertiesFile, topicName);
    }

    @Override
    public void out(String line, String lineMeta) {
        producer.send(new ProducerRecord<String, String>(topicName, line));
    }

    @Override
    public void close() {
        producer.close();
    }
}
