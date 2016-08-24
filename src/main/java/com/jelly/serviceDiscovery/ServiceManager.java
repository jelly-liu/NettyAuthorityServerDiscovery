package com.jelly.serviceDiscovery;

import com.jelly.NettyAuthorityClient;
import com.jelly.util.KeyUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

import static com.jelly.NettyAuthorityClient.channelManager;

/**
 * Created by jelly on 2016-8-24.
 */
public class ServiceManager {
    private static Logger logger= LoggerFactory.getLogger(ServiceManager.class);

    private static CuratorFramework client = null;
    private static ServiceDiscovery<InstanceDetails> serviceDiscovery;
    private static ServiceCache<InstanceDetails> serviceCache;

    public static void watch(){
        try {
            if (client == null) {
                synchronized (logger) {
                    client = CuratorFrameworkFactory.newClient(ZkServiceConf.ZK_ADDRESS, new ExponentialBackoffRetry(1000, 3));
                    client.start();
                }
            }

            if (serviceDiscovery == null) {
                synchronized (logger) {
                    ProtoBufInstanceSerializer<InstanceDetails> serializer = new ProtoBufInstanceSerializer(InstanceDetails.class);
                    serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class).client(client).basePath(ZkServiceConf.PATH).serializer(serializer).build();
                    serviceDiscovery.start();

                    serviceCache = serviceDiscovery.serviceCacheBuilder().name(ZkServiceConf.SERVICE_NAME).build();
                    serviceCache.addListener(new ServiceCacheListener() {
                        @Override
                        public void cacheChanged() {
                            try {
                                System.out.println("ServiceManager, cacheChanged, active or inactive service instance");
                                List<ServiceInstance<InstanceDetails>> serviceInstanceList = serviceCache.getInstances();
                                if (serviceInstanceList == null || serviceInstanceList.size() == 0) {
                                    System.out.println("ServiceManager, cacheChanged, can not find any server");
                                    return;
                                }

                                //close channel of all crushed server
                                for(InstanceDetails instanceDetails : channelManager.selectOutCrashServer(serviceInstanceList)){
                                    String channelKey = KeyUtil.toMD5(instanceDetails.toString());
                                    channelManager.removeChannel(channelKey);
                                }

                                //open new channel to new server
                                for(InstanceDetails  instanceDetails: channelManager.selectOutNewServer(serviceInstanceList)){
                                    String channelKey = KeyUtil.toMD5(instanceDetails.toString());
                                    if (!channelManager.containsAndAddFlag(channelKey)) {
                                        new Thread(() -> {
                                            try {
                                                NettyAuthorityClient nettyClient = new NettyAuthorityClient(NettyAuthorityClient.clientId.getAndDecrement());
                                                nettyClient.connect(instanceDetails);
                                            }catch (Exception e){
                                                e.printStackTrace();
                                            }
                                        }).start();
                                    }
                                    //avoid concurrent, in fact, do not need start client so quickly
                                    Thread.sleep(500);
                                }
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }

                        @Override
                        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                            System.out.println("Service Discovery, serviceDiscoveryCache lost connection to zookeeper");
                        }
                    });
                    serviceCache.start();
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static Collection<ServiceInstance<InstanceDetails>> queryForInstances(String serviceName) throws Exception {
        return serviceDiscovery.queryForInstances(serviceName);
    }
}
