package gr.ds.unipi.stpin.datasources;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

public class KafkaDatasource implements Datasource {

    private static final Logger logger = LoggerFactory.getLogger(KafkaDatasource.class);
    public static long buffer = 0;
    private final KafkaConsumer<String, String> consumer;
    private final String propertiesFile;
    private final String topicName;
    private final long poll;
    private Iterator<ConsumerRecord<String, String>> consumerIter;

    private KafkaDatasource(String propertiesFile, String topicName, long poll) throws IOException {
        this.propertiesFile = propertiesFile;
        this.topicName = topicName;
        this.poll = poll;

        Properties props = new Properties();
        //props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.load(new FileInputStream(propertiesFile));

        this.consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));

        this.consumerIter = consumer.poll(Duration.ofMillis(poll)).iterator();

    }

    public static KafkaDatasource newKafkaDatasource(String propertiesFile, String topicName, long poll) throws IOException {
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

            logger.info("Kafka buffer times: {}", KafkaDatasource.buffer);

            consumer.close();
            return false;
        }
    }

    @Override
    public Datasource cloneDatasource() throws IOException {
        return new KafkaDatasource(propertiesFile, topicName, poll);
    }
}
