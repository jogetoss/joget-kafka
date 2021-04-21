package org.joget.kafka;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.joget.apps.app.model.AppDefinition;
import org.joget.apps.app.service.AppUtil;
import org.joget.commons.util.PluginThread;
import org.joget.plugin.base.DefaultApplicationPlugin;

public class KafkaProducerTool extends DefaultApplicationPlugin {

    public String getName() {
        return "Kafka Producer Tool";
    }

    public String getVersion() {
        return "6.0.0";
    }

    public String getDescription() {
        return "Tool to publish a message to a Kafka topic";
    }

    public String getLabel() {
        return "Kafka Producer Tool";
    }

    public String getClassName() {
        return getClass().getName();
    }

    public String getPropertyOptions() {
        AppDefinition appDef = AppUtil.getCurrentAppDefinition();
        String appId = appDef.getId();
        String appVersion = appDef.getVersion().toString();
        Object[] arguments = new Object[]{appId, appVersion, appId, appVersion};
        return AppUtil.readPluginResource(getClass().getName(), "/properties/kafkaProducerTool.json", arguments, true, "messages/kafkaMessages");
    }

    @Override
    public Object execute(Map map) {
        String bootstrapServers = getPropertyString("bootstrapServers"); // "broker-1-t335226q7ljf7f4n.kafka.svc02.us-south.eventstreams.cloud.ibm.com:9093";
        String apiKey = getPropertyString("apiKey"); // "p9C0Fh8ZVt9Q8DQPluE6jYLYhmMmKLaKtXCjWkpSI6OZ";
        String topic = getPropertyString("topic"); // "kafka-java-console-sample-topic";
        String key = getPropertyString("key"); // "key";
        String message = getPropertyString("message"); // "message";

        // get connection properties
        Properties producerProperties = getClientConfig(bootstrapServers, apiKey);

        // set classloader for OSGI
        Thread currentThread = Thread.currentThread();
        ClassLoader threadContextClassLoader = currentThread.getContextClassLoader();
        try {
            currentThread.setContextClassLoader(this.getClass().getClassLoader());

            // start producer thread
            ProducerRunnable producerRunnable = new ProducerRunnable(producerProperties, topic, key, message);
            PluginThread producerThread = new PluginThread(producerRunnable);
            producerThread.start();
        } finally {
            // reset classloader
            currentThread.setContextClassLoader(threadContextClassLoader);
        }
        return null;
    }

    /**
     * Get connection properties to a Kafka cluster.
     * @param boostrapServers
     * @param apikey
     * @return 
     */
    public Properties getClientConfig(String boostrapServers, String apikey) {
        Properties configs = new Properties();
        // common properties
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        configs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"token\" password=\"" + apikey + "\";");
        configs.put(SslConfigs.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        configs.put(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG, "TLSv1.2");
        configs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS");

        // producer properties
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-joget-producer");
        configs.put(ProducerConfig.ACKS_CONFIG, "-1");
        configs.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");
        return configs;
    }

}
