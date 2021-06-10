package org.joget.kafka;

import java.util.Map;
import org.joget.apps.app.model.AppDefinition;
import org.joget.apps.app.service.AppUtil;
import org.joget.plugin.base.DefaultAuditTrailPlugin;

public class KafkaConsumerAuditTrail extends DefaultAuditTrailPlugin {

    @Override
    public String getName() {
        return "Kafka Consumer Audit Trail";
    }

    @Override
    public String getVersion() {
        return "6.0.1";
    }

    @Override
    public String getDescription() {
        return "Tool to consume messages from a Kafka topic";
    }

    @Override
    public String getLabel() {
        return "Kafka Consumer Audit Trail";
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
        return AppUtil.readPluginResource(getClass().getName(), "/properties/kafkaConsumerAuditTrail.json", arguments, true, "messages/kafkaMessages");
    }

    @Override
    public Object execute(Map map) {
        // no action required, background processing is handled by ConsumerThreadMonitor.
        return null;
    }
    
}
