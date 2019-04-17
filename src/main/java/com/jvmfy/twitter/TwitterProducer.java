package com.jvmfy.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private final Logger log = LoggerFactory.getLogger(TwitterProducer.class);

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1_000);
        var client = this.createTwitterClient(msgQueue);
        client.connect();

        //kafka producer
        KafkaProducer<String, String> kafkaProducer = this.createKafkaProducer();

        //shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down Twitter client...");
            client.stop();
            log.info("Closing producer...");
            kafkaProducer.close();
            log.info("Done.");
        }));

        while (!client.isDone()) {
            try {
                String msg = msgQueue.poll(5, TimeUnit.SECONDS);
                log.info(msg);
                this.sendMessage(kafkaProducer, msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
        }
    }

    private void sendMessage(KafkaProducer<String, String> kafkaProducer, String message) {
        kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, message), (recordMetadata, exception) -> {
            if (exception != null) {
                log.error("Cannot send tweet!", exception);
            }
        });
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); //ensure that exactly one copy of each message is written in the stream
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); //The number of acknowledgments the producer requires the leader to have received before considering a request complete.
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE)); //Setting a value greater than zero will cause the client to resend any record whose send fails with a potentially transient error.
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //The maximum number of unacknowledged requests the client will send on a single connection before blocking.

        //high throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // compress to snappy
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); //wait 20ms to increase chance to send more data in one batch (don't push batch immediately)
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32KB batch size

        return new KafkaProducer<>(properties);
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        var hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        var hosebirdEndpoint = new StatusesFilterEndpoint();
        var terms = Lists.newArrayList("bitcoin", "java", "kafka", "javascript", "angular", "springboot", "jvm");

        hosebirdEndpoint.trackTerms(terms);
        var hosebirdAuth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);

        var builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build(); //hosebirdClient
    }
}
