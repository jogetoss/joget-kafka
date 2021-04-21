package org.joget.kafka;

import java.util.ArrayList;
import java.util.Collection;
import org.joget.commons.util.PluginThread;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

public class Activator implements BundleActivator {

    protected Collection<ServiceRegistration> registrationList;
    PluginThread threadMonitor;
    ConsumerThreadMonitor consumerThreadMonitor;
    
    public void start(BundleContext context) {
        registrationList = new ArrayList<ServiceRegistration>();

        // register plugin here
        registrationList.add(context.registerService(KafkaProducerTool.class.getName(), new KafkaProducerTool(), null));
        registrationList.add(context.registerService(KafkaConsumerAuditTrail.class.getName(), new KafkaConsumerAuditTrail(), null));
        
        // start thread monitor
        consumerThreadMonitor = new ConsumerThreadMonitor();
        threadMonitor = new PluginThread(consumerThreadMonitor);
        threadMonitor.setDaemon(true);
        threadMonitor.start();
    }

    public void stop(BundleContext context) {
        consumerThreadMonitor.shutdown();
        try {
            Thread.sleep(10000); // delay shutdown to allow cleanup
        } catch (InterruptedException ex) {
            // ignore
        }
        for (ServiceRegistration registration : registrationList) {
            registration.unregister();
        }
    }
}