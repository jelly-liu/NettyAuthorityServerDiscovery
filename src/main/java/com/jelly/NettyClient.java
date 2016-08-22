package com.jelly;

import com.jelly.handler.AuthorityClientHandler;
import com.jelly.model.User;
import com.jelly.serviceDiscovery.InstanceDetails;
import com.jelly.serviceDiscovery.ProtoBufInstanceSerializer;
import com.jelly.serviceDiscovery.ZkServiceConf;
import com.jelly.util.KeyUtil;
import com.jelly.util.ProtoStuffSerializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jelly on 2016-8-19.
 */
public class NettyClient {
    private static Logger logger= LoggerFactory.getLogger(NettyClient.class);

    private static AtomicInteger counter = new AtomicInteger(1);

    private static CuratorFramework client = null;
    private static ServiceDiscovery<InstanceDetails> serviceDiscovery = null;
    private static ServiceCache<InstanceDetails> serviceCache;
    public static Map<String, String> serverMap = new ConcurrentHashMap<>();

    private EventLoopGroup workerGroup;
    private Channel channel;
    private String clientName;

    public NettyClient(int counter){
        this.clientName = "NettyClient-" + counter;
    }

    public void connect(InstanceDetails instanceDetails) throws Exception{
        String host = instanceDetails.getHost();
        int port = instanceDetails.getPort();

        workerGroup=new NioEventLoopGroup();
        Bootstrap b = new Bootstrap();
        b.group(workerGroup);
        b.channel(NioSocketChannel.class);
        b.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4));//1
                ch.pipeline().addLast(new ByteArrayDecoder());//2
                ch.pipeline().addLast(new LengthFieldPrepender(4));//2
                ch.pipeline().addLast(new ByteArrayEncoder());//1
                ch.pipeline().addLast(new AuthorityClientHandler(instanceDetails));
            }
        });

        int i = 5;
        while(i > 0) {
            try {
                ChannelFuture f = b.connect(host, port).sync();
                channel = f.channel();
            } catch (Exception e) {
                i--;
                e.printStackTrace();
                Thread.sleep(2000);
                continue;
            }
            break;
        }

        if(i <= 0){
            serverMap.remove(KeyUtil.toMD5(instanceDetails.toString()));
            workerGroup.shutdownGracefully();
            System.out.println("failed connect to server, host=" + host + ", port=" + port);
        }else{
            serverMap.put(KeyUtil.toMD5(instanceDetails.toString()), instanceDetails.toString());
            System.out.println("success connect to server, host=" + host + ", port=" + port);
        }
    }

    private void simulateClientRequireAuthority(){
        for (int i = 0; i < 1000; i++) {
            if(channel != null && channel.isOpen() && channel.isActive()){
                String name = this.clientName + "_abc_def_ghi__" + i;
                User user = new User(name, i);
                byte[] bytes = ProtoStuffSerializer.serialize(user);
                logger.debug("AuthorityClientHandler write msg={}", bytes);
                channel.writeAndFlush(bytes);
                try {
                    Thread.sleep(6000);
                } catch (InterruptedException e) {
                }
            }else{
                logger.debug("channel is inActive, stop");
                break;
            }
        }
    }

    private static void initCuratorClientAndServiceDiscovery(){
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
                                System.out.println("ServiceWatcher, cacheChanged, active or inactive service instance");
                                List<ServiceInstance<InstanceDetails>> serviceInstanceList = serviceCache.getInstances();
                                if (serviceInstanceList == null || serviceInstanceList.size() == 0) {
                                    System.out.println("ServiceWatcher, cacheChanged, all server crushed");
                                    return;
                                }
                                for (ServiceInstance<InstanceDetails> serviceInstance : serviceInstanceList) {
                                    InstanceDetails instanceDetails = serviceInstance.getPayload();
                                    String key = KeyUtil.toMD5(instanceDetails.toString());
                                    if (!NettyClient.serverMap.containsKey(key)) {
                                        NettyClient.serverMap.put(key, instanceDetails.toString());
                                        System.out.println("start new NettyClient, InstanceDetails=" + instanceDetails.toString());
                                        new Thread(new Runnable() {
                                            @Override
                                            public void run() {
                                                try {
                                                    NettyClient nettyClient = new NettyClient(counter.getAndDecrement());
                                                    nettyClient.connect(instanceDetails);
                                                    nettyClient.simulateClientRequireAuthority();
                                                }catch (Exception e){
                                                    e.printStackTrace();
                                                }
                                            }
                                        }).start();
                                    }
                                }
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }

                        @Override
                        public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                            System.out.println("Service Discovery, client lost connection to zookeeper");
                        }
                    });
                    serviceCache.start();
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            //init
            initCuratorClientAndServiceDiscovery();

            //init connection to server
            Collection<ServiceInstance<InstanceDetails>> serviceInstanceCollection = serviceDiscovery.queryForInstances(ZkServiceConf.SERVICE_NAME);
            if(serviceInstanceCollection == null || serviceInstanceCollection.size() == 0){
                System.out.println("can not find any ServiceInstance");
            }else{
                for(ServiceInstance<InstanceDetails> serviceInstance : serviceInstanceCollection){
                    InstanceDetails instanceDetails = serviceInstance.getPayload();
                    System.out.println("start new NettyClient, InstanceDetails=" + instanceDetails.toString());
                    new Thread(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                NettyClient nettyClient = new NettyClient(counter.getAndDecrement());
                                nettyClient.connect(instanceDetails);
                                nettyClient.simulateClientRequireAuthority();
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                        }
                    }).start();
                }
            }

            Thread.sleep(Integer.MAX_VALUE);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
