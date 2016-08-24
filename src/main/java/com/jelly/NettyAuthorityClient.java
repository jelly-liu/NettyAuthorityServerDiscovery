package com.jelly;

import com.jelly.handler.AuthorityClientHandler;
import com.jelly.serviceDiscovery.InstanceDetails;
import com.jelly.serviceDiscovery.ServiceManager;
import com.jelly.serviceDiscovery.ZkServiceConf;
import com.jelly.util.ChannelManager;
import com.jelly.util.KeyUtil;
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
import org.apache.curator.x.discovery.ServiceInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jelly on 2016-8-19.
 */
public class NettyAuthorityClient {
    private static Logger logger= LoggerFactory.getLogger(NettyAuthorityClient.class);

    public static final ChannelManager channelManager = new ChannelManager();
    public static AtomicInteger clientId = new AtomicInteger(1);

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
                channelManager.addChannel(new ChannelManager.ChannelInstance(channelKey, channel, clientName, instanceDetails, workerGroup));
                System.out.println("success connect to server, host=" + host + ", port=" + port);
            }
        }catch (Exception e){
            channelManager.removeChannel(channelKey);
            if(workerGroup != null)workerGroup.shutdownGracefully();
            e.printStackTrace();
        }
    }

    public static void openChannelToNewServer(InstanceDetails instanceDetails){
        new Thread(()->{
            try {
                System.out.println("startNewServer new NettyAuthorityClient, InstanceDetails=" + instanceDetails.toString());
                NettyAuthorityClient nettyClient = new NettyAuthorityClient(clientId.getAndDecrement());
                nettyClient.connect(instanceDetails);
            }catch (Exception e){
                e.printStackTrace();
            }
        }).start();
    }

    public static void main(String[] args) throws Exception {
        try {
            //we assumption that, there is a lot of User
            //send each User to an random serverChannel, after the server done permissions validation, client will receive result and print it
            UserAuthoritySimulator.simulateClientRequireAuthorityToServer();

            //init curatorClient, serviceDiscovery, serviceDiscoveryCache
            ServiceManager.watch();

            //init channel to all active servers
            Collection<ServiceInstance<InstanceDetails>> serviceInstanceCollection = ServiceManager.queryForInstances(ZkServiceConf.SERVICE_NAME);
            if(serviceInstanceCollection == null || serviceInstanceCollection.size() == 0){
                System.out.println("can not find any ServiceInstance");
            }else{
                for(ServiceInstance<InstanceDetails> serviceInstance : serviceInstanceCollection){
                    InstanceDetails instanceDetails = serviceInstance.getPayload();
                    openChannelToNewServer(instanceDetails);
                }
            }

            Thread.sleep(Integer.MAX_VALUE);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
