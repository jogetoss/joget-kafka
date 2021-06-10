package org.joget.kafka;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.joget.apps.app.dao.PluginDefaultPropertiesDao;
import org.joget.apps.app.model.AppDefinition;
import org.joget.apps.app.model.PluginDefaultProperties;
import org.joget.apps.app.service.AppPluginUtil;
import org.joget.apps.app.service.AppService;
import org.joget.apps.app.service.AppUtil;
import org.joget.commons.util.LogUtil;
import org.joget.commons.util.PluginThread;
import org.joget.workflow.util.WorkflowUtil;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;

/**
 * Monitoring thread that polls published apps for matching audit trail plugins.
 * New threads will be created for newly configured or modified plugins 
 * and unused threads will be stopped.
 */
public class ConsumerThreadMonitor implements Runnable {

    boolean stopThreadMonitor = false;
    int threadMonitorInterval = 10000; // 10s

    static Collection<String> pluginConfigs = new HashSet<>(); // appId_json
    static Map<String, ConsumerRunnable> runnables = new HashMap<>(); // appId -> Thread
    
    @Override
    public void run() {
        LogUtil.info(getClass().getName(), "Waiting for platform init"); 
        waitForInit();
        LogUtil.info(getClass().getName(), "Started thread monitor"); 
        try {
            while(!stopThreadMonitor) {
                monitorRunnables();
                Thread.sleep(threadMonitorInterval);
            }
            cleanup();
        } catch(InterruptedException e) {
            LogUtil.error(getClass().getName(), e, "Error checking threads" + e.getMessage());
        }
    }
    
    /**
     * Wait for completion of startup initialization for the platform.
     * This is to prevent premature thread creation on startup. 
     */
    protected void waitForInit() {
        try {
            AppUtil appUtil = null;
            ApplicationContext appContext = AppUtil.getApplicationContext();
            if (appContext != null) {
                appUtil = (AppUtil)appContext.getBean("appUtil");
            }            
            if (appUtil == null) {
                Thread.sleep(500);
                waitForInit();
            }
        } catch (InterruptedException | BeansException e) {
            LogUtil.error(getClass().getName(), e, "Error retrieving app service " + e.getMessage());
        }
    }    
    
    /**
     * Stop the thread monitor.
     */
    public void shutdown() {
        LogUtil.info(getClass().getName(), "Stopping thread monitor"); 
        stopThreadMonitor = true;
    }
    
    /**
     * Stops all running threads.
     */
    public void cleanup() {
        for (String appId: runnables.keySet()) {
            stopRunnable(appId);
        }
        runnables.clear();
        pluginConfigs.clear();
    }
    
    /**
     * Poll published apps for matching audit trail plugins.
     * New threads will be created for newly configured or modified plugins 
     * and unused threads will be stopped.
     */
    public void monitorRunnables() {
        // get published apps
        ApplicationContext appContext = AppUtil.getApplicationContext();
        AppService appService = (AppService)appContext.getBean("appService");
        Collection<AppDefinition> appDefs = appService.getPublishedApps(null);
        
        // loop thru apps to retrieve configured plugins
        String pluginName = new KafkaConsumerAuditTrail().getName();
        PluginDefaultPropertiesDao dao = (PluginDefaultPropertiesDao)appContext.getBean("pluginDefaultPropertiesDao");
        Map<String, String> pluginMap = new HashMap<>();
        for (AppDefinition appDef: appDefs) {
            Collection<PluginDefaultProperties> pluginDefaultProperties = dao.getPluginDefaultPropertiesList(pluginName, appDef, null, Boolean.TRUE, null, null);
            if (!pluginDefaultProperties.isEmpty()) {
                // get plugin config
                PluginDefaultProperties pluginProperties = pluginDefaultProperties.iterator().next();
                String appId = appDef.getAppId();
                String json = pluginProperties.getPluginProperties();
                String key = appId + "_" + json.hashCode();
                
                // start threads for new or modified plugins
                if (!pluginConfigs.contains(key)) {
                    if (runnables.containsKey(appId)) {
                        stopRunnable(appId);
                    }
                    try {
                        ConsumerRunnable newRunnable = startRunnable(appDef, json);
                        if (newRunnable != null) {
                            runnables.put(appId, newRunnable);
                        }
                    } catch(Exception e) {
                        LogUtil.error(getClass().getName(), e, "Error starting runnable " + e.getMessage());                        
                    }
                }
                pluginMap.put(appId, key);
            }
        }
        // stop threads for unconfigured plugins
        for (String appId: runnables.keySet()) {
            String key = pluginMap.get(appId);
            if (key == null) {
                stopRunnable(appId);
                runnables.remove(appId);
            }
        }
        pluginConfigs.clear();
        pluginConfigs.addAll(pluginMap.values());        
    }

    /**
     * Start a consumer thread.
     * @param appDef
     * @param json
     * @return The created Thread.
     */
    public ConsumerRunnable startRunnable(AppDefinition appDef, String json) {
        ConsumerRunnable runnable = null;
        
        // get plugin configuration
        Map propertiesMap = AppPluginUtil.getDefaultProperties(new KafkaConsumerAuditTrail(), json, appDef, null);        
        String bootstrapServers = WorkflowUtil.processVariable((String)propertiesMap.get("bootstrapServers"), "", null);
        String clientId = WorkflowUtil.processVariable((String)propertiesMap.get("clientId"), "", null);
        String apiKey = WorkflowUtil.processVariable((String)propertiesMap.get("apiKey"), "", null);
        String topic = WorkflowUtil.processVariable((String)propertiesMap.get("topic"), "", null);
        String script = WorkflowUtil.processVariable((String)propertiesMap.get("script"), "", null);
        boolean debug =  "true".equalsIgnoreCase((String)propertiesMap.get("debugMode"));

        // get connection properties
        Properties consumerProperties = getClientConfig(bootstrapServers, clientId, apiKey);

        // set classloader for OSGI
        Thread currentThread = Thread.currentThread();
        ClassLoader threadContextClassLoader = currentThread.getContextClassLoader();        
        try {
            currentThread.setContextClassLoader(this.getClass().getClassLoader());

            // start thread
            runnable = new ConsumerRunnable(consumerProperties, topic, script, debug);
            PluginThread thread = new PluginThread(runnable);
            thread.start();
        } finally {
            // reset classloader
            currentThread.setContextClassLoader(threadContextClassLoader);
        }
        
        LogUtil.info(getClass().getName(), "Started Kafka consumer thread for app " + appDef.getAppId()); 
        if (debug) {
            LogUtil.info(getClass().getName(), "Kafka consumer thread JSON: " + json); 
        }
        return runnable;
    }
    
    /**
     * Stop a consumer thread.
     * @param appId 
     */
    public void stopRunnable(String appId) {
        ConsumerRunnable runnable = runnables.get(appId);
        if (runnable != null) {
            runnable.shutdown();
            LogUtil.info(getClass().getName(), "Stopped Kafka consumer thread for " + appId); 
        }
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

        // consumer properties
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-java-console-sample-consumer");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-console-sample-group");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configs.put(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG,"use_all_dns_ips");
        return configs;
    }    

}
