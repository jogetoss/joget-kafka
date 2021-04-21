package org.joget.kafka;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.joget.commons.util.LogUtil;

/**
 * Adapted from the samples at
 * https://github.com/ibm-messaging/event-streams-samples
 */
public class ProducerRunnable implements Runnable {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;
    private volatile boolean closing = false;
    private final String key;
    private final String message;

    public ProducerRunnable(Properties producerProperties, String topic, String key, String message) {
        this.topic = topic;
        this.key = key;
        this.message = message;

        // Create a Kafka producer with the provided client configuration
        kafkaProducer = new KafkaProducer<String, String>(producerProperties);

        try {
            // Checking for topic existence.
            // If the topic does not exist, the kafkaProducer will retry for about 60 secs
            // before throwing a TimeoutException
            // see configuration parameter 'metadata.fetch.timeout.ms'
            List<PartitionInfo> partitions = kafkaProducer.partitionsFor(topic);
            LogUtil.info(getClass().getName(), partitions.toString());
        } catch (TimeoutException kte) {
            LogUtil.error(getClass().getName(), kte, "Topic '" + topic + "' may not exist - application will terminate");
            kafkaProducer.close();
            throw new IllegalStateException("Topic '" + topic + "' may not exist - application will terminate", kte);
        }
    }

    @Override
    public void run() {
        LogUtil.info(getClass().getName(), ProducerRunnable.class.toString() + " is starting.");

        try {
            while (!closing) {
                try {
                    // If a partition is not specified, the client will use the default partitioner to choose one.
                    ProducerRecord<String, String> record = new ProducerRecord<String, String>(
                            topic, key, message);

                    // Send record asynchronously
                    Future<RecordMetadata> future = kafkaProducer.send(record);

                    // Synchronously wait for a response from Event Streams / Kafka on every message produced.
                    // For high throughput the future should be handled asynchronously.
                    RecordMetadata recordMetadata = future.get(5000, TimeUnit.MILLISECONDS);

                    LogUtil.info(getClass().getName(), "Message produced, offset: " + recordMetadata.offset());

                    shutdown();

                } catch (final InterruptedException e) {
                    LogUtil.warn(getClass().getName(), "Producer closing - caught exception: " + e);
                } catch (final Exception e) {
                    LogUtil.error(getClass().getName(), e, "Sleeping for 5s - Producer has caught : " + e);
                    try {
                        Thread.sleep(5000); // Longer sleep before retrying
                    } catch (InterruptedException e1) {
                        LogUtil.warn(getClass().getName(), "Producer closing - caught exception: " + e);
                    }
                }
            }
        } finally {
            kafkaProducer.close(5000, TimeUnit.MILLISECONDS);
            LogUtil.info(getClass().getName(), ProducerRunnable.class.toString() + " has shut down.");
        }
    }

    public void shutdown() {
        closing = true;
        LogUtil.info(getClass().getName(), ProducerRunnable.class.toString() + " is shutting down.");
    }
}
