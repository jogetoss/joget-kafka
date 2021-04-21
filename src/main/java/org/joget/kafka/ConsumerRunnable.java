package org.joget.kafka;

import bsh.Interpreter;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.joget.commons.util.LogUtil;

/**
 * Adapted from the samples at
 * https://github.com/ibm-messaging/event-streams-samples Kafka consumer
 * runnable which can be used to create and run consumer as a separate thread.
 */
public class ConsumerRunnable implements Runnable {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private boolean closing = false;
    private String script;
    private boolean debug;

    public ConsumerRunnable(Properties clientConfiguration, String broker, String apikey, String topic, String script, boolean debugMode) {
        this.script = script;
        this.debug = debugMode;
        this.kafkaConsumer = new KafkaConsumer<>(clientConfiguration);
        this.kafkaConsumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(Collection<org.apache.kafka.common.TopicPartition> partitions) {
                try {
                    LogUtil.info(getClass().getName(), "Partitions " + partitions + " assigned, consumer seeking to end.");

                    for (TopicPartition partition : partitions) {
                        long position = kafkaConsumer.position(partition);
                        if (debug) {
                            LogUtil.info(getClass().getName(), "current Position: " + position);
                            LogUtil.info(getClass().getName(), "Seeking to end...");
                        }
                        kafkaConsumer.seekToEnd(Arrays.asList(partition));
                        if (debug) {
                            LogUtil.info(getClass().getName(), "Seek from the current position: " + kafkaConsumer.position(partition));
                        }
                        kafkaConsumer.seek(partition, position);
                    }
                    LogUtil.info(getClass().getName(), "Producer can now begin producing messages.");
                } catch (final Exception e) {
                    LogUtil.error(getClass().getName(), e, "Error when assigning partitions");
                }

            }
        });
    }

    @Override
    public void run() {
        LogUtil.info(getClass().getName(), "Consumer is starting.");

        while (!closing) {
            try {
                Duration duration = Duration.of(1, ChronoUnit.SECONDS);
                Iterator<ConsumerRecord<String, String>> it = this.kafkaConsumer.poll(duration).iterator();

                // Iterate through all the messages received and print their content.
                while (it.hasNext()) {
                    ConsumerRecord<String, String> record = it.next();
                    String key = record.key();
                    String value = record.value();
                    LogUtil.info(getClass().getName(), "Executing script. Offset: " + record.offset());
                    if (debug) {
                        LogUtil.info(getClass().getName(), "Key=" + key + ". Value=" + value);
                    }
                    executeScript(script, key, value);
                }

                Thread.sleep(1000);
            } catch (final InterruptedException e) {
                LogUtil.error(getClass().getName(), e, "Producer/Consumer loop has been unexpectedly interrupted");
                shutdown();
            } catch (final Exception e) {
                LogUtil.error(getClass().getName(), e, "Consumer has failed with exception: " + e);
                shutdown();
            }
        }

        LogUtil.info(getClass().getName(), "Consumer is shutting down.");
        this.kafkaConsumer.close();
    }

    public void shutdown() {
        closing = true;
    }

    /**
     * Execute a BeanShell script with injected values for key and value.
     * @param script
     * @param key
     * @param value 
     */
    public void executeScript(String script, String key, String value) {
        try {
            Interpreter interpreter = new Interpreter();
            interpreter.setClassLoader(getClass().getClassLoader());
            interpreter.set("key", key);
            interpreter.set("value", value);
            
            Object result = interpreter.eval(script);
            LogUtil.info(getClass().getName(), "Script execution complete.");
            if (debug) {
                LogUtil.info(getClass().getName(), "Script result: " + result);
            }
        } catch (Exception e) {
            LogUtil.error(getClass().getName(), e, "Error executing script");
        }
    }
}
