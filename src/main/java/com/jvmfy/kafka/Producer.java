package com.jvmfy.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class Producer {

    public static void main(String[] args) {

        final var log = LoggerFactory.getLogger(Producer.class);

        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        IntStream.range(0, 10).forEach(i ->
                producer.send(new ProducerRecord<>("test_topic", "key " + 1, "test " + i), (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Received: \n Topic: {}, \n Partition: {} \n Offset: {} \n Timestamp: {}\n", metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
                        return;
                    }
                    log.error("Error while producing", exception);
                })
        );

        producer.flush();
        producer.close();
    }
}
