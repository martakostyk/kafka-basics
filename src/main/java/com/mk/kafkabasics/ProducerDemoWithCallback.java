package com.mk.kafkabasics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final String BOOTSTRAP_SERVERS = "127.0.01:9092";
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world");

        // send data
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    log.info(String.format(
                            "Message produced (topic: %s, partition: %s, offset: %d, timestamp: %d)",
                            recordMetadata.topic(),
                            recordMetadata.partition(),
                            recordMetadata.offset(),
                            recordMetadata.timestamp()));
                } else {
                    log.error("Error while producing the message", e);
                }
            }
        });

        // flush data
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
