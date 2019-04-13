package com.jvmfy.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
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

public class Consumer {

    public static void main(String[] args) {
        new Consumer().run();
    }

    private void run() {
        var log = LoggerFactory.getLogger(Consumer.class);
        var bootstrapServer = "127.0.0.1:9092";
        var groupId = "test_group";
        var topic = "test_topic";

        CountDownLatch latch = new CountDownLatch(1);
        var consumerRunnable = new ConsumerRunnable(
                bootstrapServer,
                groupId,
                topic,
                latch
        );

        var consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            consumerRunnable.shutdown();
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

        private final Logger log = LoggerFactory.getLogger(ConsumerRunnable.class);
        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;

        private ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {
            var properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            this.consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singleton(topic));

            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    records.forEach(r -> log.info("Key: {} \n Value: {} \n Partition: {} \n Offset: {} \n", r.key(), r.value(), r.partition(), r.offset()));
                }
            } catch (WakeupException e) {
                log.info("Received shutdown signal");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        void shutdown() {
            consumer.wakeup();
        }
    }
}
