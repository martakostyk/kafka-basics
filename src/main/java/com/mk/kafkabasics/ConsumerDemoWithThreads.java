package com.mk.kafkabasics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads() {
    }

    private void run() {
        Logger log = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-sixth-app";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);

        log.info("Creating the consumer thread");
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);
        Thread consumerThread = new Thread(consumerRunnable);

        log.info("Starting consumer thread");
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            consumerRunnable.shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Application got interrupted", e);
        } finally {
            log.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private Logger log = LoggerFactory.getLogger(ConsumerRunnable.class);
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while(true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord record : records) {
                        log.info("key: " + record.key() + ", value: " + record.value());
                        log.info("partition: " + record.partition() + ", offset: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                log.info("Received shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        void shutDown() {
            consumer.wakeup();
        }
    }
}
