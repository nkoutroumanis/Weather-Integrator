package com.github.nkoutroumanis;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaParser implements Parser {

    private final KafkaConsumer<String, String> consumer;
    private Iterator<ConsumerRecord<String, String>> consumerIter;

    private final String propertiesFile;
    private final String topicName;
    private final long poll;

    private KafkaParser(String propertiesFile, String topicName, long poll) throws IOException {
        this.propertiesFile = propertiesFile;
        this.topicName = topicName;
        this.poll = poll;

        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.load(new FileInputStream(propertiesFile));
        
        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));

        this.consumerIter = consumer.poll(Duration.ofMinutes(poll)).iterator();

    }

    public static KafkaParser newKafkaParser(String propertiesFile, String topicName, long poll) throws IOException {
        return new KafkaParser(propertiesFile, topicName, poll);
    }


    @Override
    public String[] nextLine() {
        ConsumerRecord<String, String> record = consumerIter.next();
        return new String[] {record.value(), /*record.key()*/"kafkaTopicData.csv"};
    }

    @Override
    public boolean hasNextLine() {

        if(consumerIter.hasNext()){
            return true;
        }
        else{

            this.consumerIter = consumer.poll(Duration.ofMinutes(poll)).iterator();

            if(consumerIter.hasNext()){
                System.out.println("poll runs again");
                return true;
            }
            consumer.close();
            return false;
        }
    }
}
