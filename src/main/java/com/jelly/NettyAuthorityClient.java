package com.jelly;

import com.jelly.handler.AuthorityClientHandler;
import com.jelly.model.User;
import com.jelly.serviceDiscovery.InstanceDetails;
import com.jelly.serviceDiscovery.ProtoBufInstanceSerializer;
import com.jelly.serviceDiscovery.ZkServiceConf;
import com.jelly.util.KeyUtil;
import com.jelly.util.ProtoStuffSerializer;
import com.jelly.util.ChannelManager;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jelly on 2016-8-19.
 */
public class NettyAuthorityClient {
    private static Logger logger= LoggerFactory.getLogger(NettyAuthorityClient.class);

    public static final ChannelManager channelManager = new ChannelManager();
    private static AtomicInteger clientId = new AtomicInteger(1);

    private static CuratorFramework client = null;
    private static ServiceDiscovery<InstanceDetails> serviceDiscovery;
    private static ServiceCache<InstanceDetails> serviceCache;

    private EventLoopGroup workerGroup;
    private String clientName;

    public NettyAuthorityClient(int counter){
        this.clientName = "NettyAuthorityClient-" + counter;
    }

    public void connect(InstanceDetails instanceDetails) throws Exception{
        String channelKey = KeyUtil.toMD5(instanceDetails.toString());

        try {
            System.out.println("startNewServer new NettyAuthorityClient, InstanceDetails=" + instanceDetails.toString());
            String host = instanceDetails.getHost();
            int port = instanceDetails.getPort();

            workerGroup = new NioEventLoopGroup();
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

            ChannelFuture channelFuture = null;
            Channel channel = null;
            int i = 0;
            while (i < 5) {
                System.out.println("try " + i + " times to connect to server, host=" + host + ", port=" + port);
                try {
                    channelFuture = b.connect(host, port).sync();
                    channel = channelFuture.channel();
                } catch (Exception e) {
                    i++;
                    e.printStackTrace();
                    Thread.sleep(2000);
                    continue;
                }
                break;
            }

            if (i >= 5 || channel == null) {
                channelManager.removeChannel(channelKey);
                workerGroup.shutdownGracefully();
                System.out.println("failed connect to server, host=" + host + ", port=" + port);
            } else {
                channelManager.addChannel(channelKey, channel, instanceDetails);
                System.out.println("success connect to server, host=" + host + ", port=" + port);
            }
        }catch (Exception e){
            channelManager.removeChannel(channelKey);
            if(workerGroup != null)workerGroup.shutdownGracefully();
            e.printStackTrace();
        }
    }

    private void simulateClientRequireAuthority(){
        ChannelManager.ChannelInstance channelInstance = null;
        for (int i = 0; i < 1000; i++) {
            channelInstance = channelManager.getRoundRobinChannel();
            Channel channel = channelInstance.channel;
            InstanceDetails instanceDetails = channelInstance.instanceDetails;
            if(channel != null && channel.isOpen() && channel.isActive()){
                String name = instanceDetails.toString() + "__" + this.clientName + "__abc_def_ghi__" + i;
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
                                    System.out.println("ServiceWatcher, cacheChanged, can not find any server");
                                    return;
                                }
                                for (ServiceInstance<InstanceDetails> serviceInstance : serviceInstanceList) {
                                    InstanceDetails instanceDetails = serviceInstance.getPayload();
                                    String channelKey = KeyUtil.toMD5(instanceDetails.toString());
                                    if (!channelManager.containsAndAddFlag(channelKey)) {
                                        new Thread(() -> {
                                            try {
                                                NettyAuthorityClient nettyClient = new NettyAuthorityClient(clientId.getAndDecrement());
                                                nettyClient.connect(instanceDetails);
                                                nettyClient.simulateClientRequireAuthority();
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

    public static void startNewClient(InstanceDetails instanceDetails){
        new Thread(()->{
            try {
                System.out.println("startNewServer new NettyAuthorityClient, InstanceDetails=" + instanceDetails.toString());
                NettyAuthorityClient nettyClient = new NettyAuthorityClient(clientId.getAndDecrement());
                nettyClient.connect(instanceDetails);
                nettyClient.simulateClientRequireAuthority();
            }catch (Exception e){
                e.printStackTrace();
            }
        }).start();
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
                    startNewClient(instanceDetails);
                }
            }

            Thread.sleep(Integer.MAX_VALUE);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
