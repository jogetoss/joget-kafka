package org.joget.kafka;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.joget.apps.app.model.AppDefinition;
import org.joget.apps.app.service.AppUtil;
import org.joget.commons.util.PluginThread;
import org.joget.plugin.base.DefaultApplicationPlugin;
import org.joget.workflow.model.WorkflowAssignment;
import org.joget.workflow.util.WorkflowUtil;

public class KafkaProducerTool extends DefaultApplicationPlugin {

    @Override
    public String getName() {
        return "Kafka Producer Tool";
    }

    @Override
    public String getVersion() {
        return "6.0.1";
    }

    @Override
    public String getDescription() {
        return "Tool to publish a message to a Kafka topic";
    }

    @Override
    public String getLabel() {
        return "Kafka Producer Tool";
    }

    @Override
    public String getClassName() {
        return getClass().getName();
    }

    @Override
    public String getPropertyOptions() {
        AppDefinition appDef = AppUtil.getCurrentAppDefinition();
        String appId = appDef.getId();
        String appVersion = appDef.getVersion().toString();
        Object[] arguments = new Object[]{appId, appVersion, appId, appVersion};
        return AppUtil.readPluginResource(getClass().getName(), "/properties/kafkaProducerTool.json", arguments, true, "messages/kafkaMessages");
    }

    @Override
    public Object execute(Map map) {
        WorkflowAssignment wfAssignment = (WorkflowAssignment)map.get("workflowAssignment");
        String bootstrapServers = WorkflowUtil.processVariable(getPropertyString("bootstrapServers"), "", wfAssignment);
        String clientId = WorkflowUtil.processVariable(getPropertyString("clientId"), "", wfAssignment);
        String apiKey = WorkflowUtil.processVariable(getPropertyString("apiKey"), "", wfAssignment);
        String topic = WorkflowUtil.processVariable(getPropertyString("topic"), "", wfAssignment);
        String key = WorkflowUtil.processVariable(getPropertyString("key"), "", wfAssignment);
        String message = WorkflowUtil.processVariable(getPropertyString("message"), "", wfAssignment);

        // get connection properties
        Properties producerProperties = getClientConfig(bootstrapServers, clientId, apiKey);

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
     * @param clientId
     * @param apikey
     * @return 
     */
    public Properties getClientConfig(String boostrapServers, String clientId, String apikey) {
        Properties configs = new Properties();
        // common properties
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        configs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        configs.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + clientId + "\" password=\"" + apikey + "\";");

        // producer properties
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "kafka-joget-producer");
        configs.put(ProducerConfig.ACKS_CONFIG, "-1");
        configs.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips");
        return configs;
    }

}
