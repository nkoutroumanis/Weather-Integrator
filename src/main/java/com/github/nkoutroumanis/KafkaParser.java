package com.github.nkoutroumanis;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaParser implements Parser {

    private final KafkaConsumer<String, String> consumer;
    private Iterator<ConsumerRecord<String, String>> consumerIter;

    private final String topicName;
    private final String servers;

    private KafkaParser(String topicName, String servers){
        this.topicName = topicName;
        this.servers = servers;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(topicName));
        this.consumerIter = consumer.poll(Duration.ofMinutes(5)).iterator();

//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(100).;
//            for (ConsumerRecord<String, String> record : records)
//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
//        }

    }

    public static KafkaParser newKafkaParser(String topicName, String servers){
        return new KafkaParser(topicName, servers);
    }


    @Override
    public String[] nextLine() {
        ConsumerRecord<String, String> record = consumerIter.next();
        return new String[] {record.value(), record.key()};
    }

    @Override
    public boolean hasNextLine() throws IOException {
        if(consumerIter.hasNext()){
            return consumerIter.hasNext();
        }
        consumer.close();
        return false;
    }
}
