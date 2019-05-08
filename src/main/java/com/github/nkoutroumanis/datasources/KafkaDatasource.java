package com.github.nkoutroumanis.datasources;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

public class KafkaDatasource implements Datasource {

    private final KafkaConsumer<String, String> consumer;
    private Iterator<ConsumerRecord<String, String>> consumerIter;

    private final String propertiesFile;
    private final String topicName;
    private final long poll;

    private KafkaDatasource(String propertiesFile, String topicName, long poll) throws IOException {
        this.propertiesFile = propertiesFile;
        this.topicName = topicName;
        this.poll = poll;

        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.load(new FileInputStream(propertiesFile));

        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));

        this.consumerIter = consumer.poll(Duration.ofMillis(poll)).iterator();

    }

    public static KafkaDatasource newKafkaParser(String propertiesFile, String topicName, long poll) throws IOException {
        return new KafkaDatasource(propertiesFile, topicName, poll);
    }


    @Override
    public String[] nextLine() {
        ConsumerRecord<String, String> record = consumerIter.next();
        return new String[]{record.value(), record.topic() + "-" + record.partition() + ".csv"/*"kafkaTopicData.csv"*/};
    }

    @Override
    public boolean hasNextLine() {

        if (consumerIter.hasNext()) {
            return true;
        } else {

            this.consumerIter = consumer.poll(Duration.ofMillis(poll)).iterator();

            if (consumerIter.hasNext()) {
                buffer++;
                return true;
            }
            consumer.close();
            return false;
        }
    }

    public static long buffer = 0;
}
