package com.jelly.util;

import com.jelly.serviceDiscovery.InstanceDetails;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.apache.commons.lang3.StringUtils;
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
    //key=MD5(host:port)
    private Set<String> channelKeySet = new HashSet<>();
    //key=MD5(host:port)
    private List<String> channelKeyList = new CopyOnWriteArrayList<>();
    //key=MD5(host:port)
    private Map<String, ChannelInstance> channelInstanceMap = new ConcurrentHashMap<>();

    public static class ChannelInstance {
        public String channelKey;
        public Channel channel;
        public String channelName;
        public InstanceDetails instanceDetails;
        public EventLoopGroup workerGroup;

        public ChannelInstance(String channelKey, Channel channel, String channelName, InstanceDetails instanceDetails, EventLoopGroup workerGroup) {
            this.channelKey = channelKey;
            this.channel = channel;
            this.channelName = channelName;
            this.instanceDetails = instanceDetails;
            this.workerGroup = workerGroup;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ChannelInstance that = (ChannelInstance) o;

            return channelKey.equals(that.channelKey);

        }

        @Override
        public int hashCode() {
            return channelKey.hashCode();
        }

        @Override
        public String toString() {
            return "ChannelInstance{" +
                    "channelKey='" + channelKey + '\'' +
                    ", channel=" + channel +
                    ", channelName='" + channelName + '\'' +
                    ", instanceDetails=" + instanceDetails +
                    ", workerGroup=" + workerGroup +
                    '}';
        }
    }

    public ChannelInstance getRoundRobinChannel(){
        if(channelKeyList == null || channelKeyList.size() == 0){
            return null;
        }

        String channelKey = null;
        ChannelInstance channelInstance = null;
        while(channelInstance == null) {
            try {
                if(channelKeyList.size() == 0){
                    return null;
                }
                int number = roundRobinLong.getAndIncrement();
                int index = number % channelKeyList.size();
                System.out.println("index=number/size, [" + number + "/" + channelKeyList.size() + "]" + ", index=" + index);
                channelKey = channelKeyList.get(index);
                channelInstance = channelInstanceMap.get(channelKey);
            }catch (Exception e){
                channelInstance = null;
                e.printStackTrace();
            }
        }

        System.out.println("use roundRobin algorithm to find an channelInstance=" + channelInstance);
        return channelInstance;
    }

    public synchronized boolean containsAndAddFlag(String channelKey){
        boolean contains = channelKeySet.contains(channelKey);
        if(!contains){
            channelKeySet.add(channelKey);
        }
        return contains;
    }

    public synchronized void addChannel(ChannelInstance channelInstance){
        channelKeySet.add(channelInstance.channelKey);
        channelKeyList.add(channelInstance.channelKey);
        channelInstanceMap.put(channelInstance.channelKey, channelInstance);
    }

    public synchronized void removeChannel(String channelKey){
        channelKeySet.remove(channelKey);
        channelKeyList.remove(channelKey);
        ChannelInstance channelInstance = channelInstanceMap.remove(channelKey);
        if(channelInstance == null){
            return;
        }

        if(channelInstance.channel != null){
            channelInstance.channel.close();
        }
        if(channelInstance.workerGroup != null){
            channelInstance.workerGroup.shutdownGracefully();
        }
    }

    public List<InstanceDetails> selectOutCrashServer(List<ServiceInstance<InstanceDetails>> latestServiceInstanceList){
        List<InstanceDetails> crashServiceInstanceList = new ArrayList<>();
        for(String channelKey : this.channelInstanceMap.keySet()){
            boolean crashServer = true;
            for(ServiceInstance<InstanceDetails> serviceInstance2 : latestServiceInstanceList){
                InstanceDetails instanceDetails2 = serviceInstance2.getPayload();
                if(StringUtils.equals(channelKey, KeyUtil.toMD5(instanceDetails2.toString()))){
                    crashServer = false;
                    break;
                }
            }
            if(crashServer){
                ChannelInstance channelInstance = channelInstanceMap.get(channelKey);
                if(channelInstance != null){
                    if(channelInstance.instanceDetails != null){
                        crashServiceInstanceList.add(channelInstance.instanceDetails);
                    }
                }
            }
        }
        return crashServiceInstanceList;
    }

    public synchronized List<InstanceDetails> selectOutNewServer(List<ServiceInstance<InstanceDetails>> latestServiceInstanceList){
        List<InstanceDetails> newServiceInstanceList = new ArrayList<>();
        Set<String> channelKeySet = channelInstanceMap.keySet();
        for(ServiceInstance<InstanceDetails> serviceInstance : latestServiceInstanceList){
            InstanceDetails instanceDetails = serviceInstance.getPayload();
            boolean newServer = false;
            if(!channelKeySet.contains(KeyUtil.toMD5(instanceDetails.toString()))){
                newServer = true;
            }
            if(newServer){
                newServiceInstanceList.add(serviceInstance.getPayload());
            }
        }
        return newServiceInstanceList;
    }
}
