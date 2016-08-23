package com.jelly.util;

import com.jelly.serviceDiscovery.InstanceDetails;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.apache.curator.x.discovery.ServiceInstance;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jelly on 2016-8-23.
 */
public class ChannelManager {
    private AtomicInteger roundRobinLong = new AtomicInteger(0);
    private List<ServiceInstance<InstanceDetails>> serviceInstanceList = new CopyOnWriteArrayList<>();
    private Set<String> channelKeySet = new HashSet();
    private List<String> channelKeyList = new CopyOnWriteArrayList<>();
    private Map<String, Channel> channelMap = new ConcurrentHashMap<>();
    private Map<String, InstanceDetails> channelInstanceMap = new ConcurrentHashMap<>();
    private Map<String, EventLoopGroup> workerGroupMap = new ConcurrentHashMap<>();

    public static class ChannelInstance {
        public Channel channel;
        public InstanceDetails instanceDetails;

        public ChannelInstance(Channel channel, InstanceDetails instanceDetails) {
            this.channel = channel;
            this.instanceDetails = instanceDetails;
        }
    }

    public ChannelInstance getRoundRobinChannel(){
        if(channelKeyList == null || channelKeyList.size() == 0){
            return null;
        }

        Channel channel = null;
        InstanceDetails instanceDetails = null;

        String channelKey = null;
        while(channel == null) {
            try {
                if(channelKeyList.size() == 0){
                    return null;
                }
                int number = roundRobinLong.getAndIncrement();
                int index = number % channelKeyList.size();
                System.out.println("index=number/size, [" + number + "/" + channelKeyList.size() + "]" + ", index=" + index);
                channelKey = channelKeyList.get(index);
                instanceDetails = channelInstanceMap.get(channelKey);
                channel = channelMap.get(channelKey);
            }catch (Exception e){
                channel = null;
                e.printStackTrace();
            }
        }

        System.out.println("use roundRobin algorithm to find an channel, channelKey=" + channelKey + ", instanceDetails=" + instanceDetails);
        return new ChannelInstance(channel, instanceDetails);
    }

    public synchronized boolean containsAndAddFlag(String channelKey){
        boolean contains = channelKeySet.contains(channelKey);
        if(!contains){
            channelKeySet.add(channelKey);
        }
        return contains;
    }

    public synchronized void addChannel(String channelKey, Channel channel, InstanceDetails instanceDetails, EventLoopGroup workerGroup){
        channelKeySet.add(channelKey);
        channelKeyList.add(channelKey);
        channelMap.put(channelKey, channel);
        channelInstanceMap.put(channelKey, instanceDetails);
        workerGroupMap.put(channelKey, workerGroup);
    }

    public synchronized void removeChannel(String channelKey){
        channelKeySet.remove(channelKey);
        channelKeyList.remove(channelKey);
        channelInstanceMap.remove(channelKey);
        Channel channel = channelMap.remove(channelKey);
        if(channel != null){
            channel.close();
        }
        EventLoopGroup workerGroup = workerGroupMap.get(channelKey);
        if(workerGroup != null){
            workerGroup.shutdownGracefully();
        }
    }

    public List<ServiceInstance<InstanceDetails>> findCrashServer(List<ServiceInstance<InstanceDetails>> currentServiceInstanceList){
        List<ServiceInstance<InstanceDetails>> crashServiceInstanceList = new ArrayList<>();
        for(ServiceInstance<InstanceDetails> serviceInstance : serviceInstanceList){
            InstanceDetails instanceDetails = serviceInstance.getPayload();
            boolean crashServer = true;
            for(ServiceInstance<InstanceDetails> serviceInstance2 : currentServiceInstanceList){
                InstanceDetails instanceDetails2 = serviceInstance2.getPayload();
                if(instanceDetails.equals(instanceDetails2)){
                    crashServer = false;
                }
            }
            if(crashServer){
                crashServiceInstanceList.add(serviceInstance);
            }
        }
        return crashServiceInstanceList;
    }

    public synchronized List<ServiceInstance<InstanceDetails>> findNewServer(List<ServiceInstance<InstanceDetails>> currentServiceInstanceList){
        List<ServiceInstance<InstanceDetails>> newServiceInstanceList = new ArrayList<>();
        for(ServiceInstance<InstanceDetails> serviceInstance : currentServiceInstanceList){
            InstanceDetails instanceDetails = serviceInstance.getPayload();
            boolean newServer = true;
            for(ServiceInstance<InstanceDetails> serviceInstance2 : serviceInstanceList){
                InstanceDetails instanceDetails2 = serviceInstance2.getPayload();
                if(instanceDetails.equals(instanceDetails2)){
                    newServer = false;
                }
            }
            if(newServer){
                newServiceInstanceList.add(serviceInstance);
            }
        }
        return newServiceInstanceList;
    }
}
