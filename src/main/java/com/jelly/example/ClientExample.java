package com.jelly.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.JsonInstanceSerializer;
import org.apache.curator.x.discovery.details.ServiceCacheListener;

import java.util.List;


/**
 * Created by jelly on 2016-8-22.
 */
public class ClientExample {
    public static void main(String[] args) throws Exception {
        CuratorFramework client = null;
        ServiceDiscovery<InstanceDetailsExample> serviceDiscovery = null;
        try {
            client = CuratorFrameworkFactory.newClient(ZkConfigExample.ZK_ADDRESS, new ExponentialBackoffRetry(1000, 3));
            client.start();

            JsonInstanceSerializer<InstanceDetailsExample> serializer = new JsonInstanceSerializer(InstanceDetailsExample.class);
            serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetailsExample.class).client(client).basePath(ZkConfigExample.PATH).serializer(serializer).build();
            serviceDiscovery.start();

            printCurrentServerList((List<ServiceInstance<InstanceDetailsExample>>)serviceDiscovery.queryForInstances(ZkConfigExample.SERVICE_NAME));

            final ServiceCache<InstanceDetailsExample> serviceCache = serviceDiscovery.serviceCacheBuilder().name(ZkConfigExample.SERVICE_NAME).build();
            serviceCache.addListener(new ServiceCacheListener() {
                @Override
                public void cacheChanged() {
                    System.out.println("ServiceWatcher, cacheChanged, active or inactive service instance");
                    printCurrentServerList(serviceCache.getInstances());
                }

                @Override
                public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                    System.out.println("Service Discovery, client lost connection to zookeeper");
                }
            });
            serviceCache.start();
            Thread.sleep(Integer.MAX_VALUE);
        } finally {
            CloseableUtils.closeQuietly(serviceDiscovery);
            CloseableUtils.closeQuietly(client);
        }
    }

    private static void printCurrentServerList(List<ServiceInstance<InstanceDetailsExample>> serviceInstanceList) {
        if(serviceInstanceList == null || serviceInstanceList.size() == 0){
            System.out.println("ServiceWatcher, but can not find any service of this service name=" + ZkConfigExample.SERVICE_NAME);
            return;
        }

        for(ServiceInstance<InstanceDetailsExample> serviceInstance : serviceInstanceList){
            System.out.println("ServiceWatcher, find service of this service name=" + ZkConfigExample.SERVICE_NAME + ", instanceDetails=" + serviceInstance.getPayload());
        }
    }
}
