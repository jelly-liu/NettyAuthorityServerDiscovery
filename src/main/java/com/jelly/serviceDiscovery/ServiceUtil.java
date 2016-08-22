package com.jelly.serviceDiscovery;

import com.jelly.util.KeyUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;

/**
 * Created by jelly on 2016-8-22.
 */
public class ServiceUtil {
    private static CuratorFramework client = null;

    public static void checkAndRegister(CuratorFramework client, InstanceDetails instanceDetails) {
        try {
            //register again
            ServiceInstance<InstanceDetails> serviceInstance = ServiceInstance.<InstanceDetails>builder()
                    .name(ZkServiceConf.SERVICE_NAME)
                    .payload(instanceDetails)
                    .build();
            ProtoBufInstanceSerializer<InstanceDetails> serializer = new ProtoBufInstanceSerializer(InstanceDetails.class);
            ServiceDiscovery<InstanceDetails> serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class)
                    .client(client)
                    .basePath(ZkServiceConf.PATH)
                    .serializer(serializer)
                    .build();

            serviceDiscovery.registerService(serviceInstance);
            serviceDiscovery.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
