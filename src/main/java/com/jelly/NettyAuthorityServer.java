package com.jelly;

import com.jelly.handler.AuthorityServerHandler;
import com.jelly.serviceDiscovery.InstanceDetails;
import com.jelly.serviceDiscovery.ProtoBufInstanceSerializer;
import com.jelly.serviceDiscovery.ZkServiceConf;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.log4j.Logger;

import java.util.Random;

/**
 * Created by jelly on 2016-8-19.
 */
public class NettyAuthorityServer {
    private static Logger logger = Logger.getLogger(NettyAuthorityServer.class);

    private CuratorFramework client;

    public void startNewServer(InstanceDetails instanceDetails) throws Exception {
        String host = instanceDetails.getHost();
        int port = instanceDetails.getPort();

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(bossGroup, workerGroup);
            serverBootstrap.channel(NioServerSocketChannel.class);
            serverBootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4));//1
                    ch.pipeline().addLast(new ByteArrayDecoder());//2
                    ch.pipeline().addLast(new LengthFieldPrepender(4));//2
                    ch.pipeline().addLast(new ByteArrayEncoder());//1
                    ch.pipeline().addLast(new AuthorityServerHandler(instanceDetails));
                }
            });

            // Start the server.
            ChannelFuture channelFuture = serverBootstrap.bind(host, port).sync();
            //register server
            registerServer(instanceDetails);
            // Wait until the server socket is closed.
            System.out.println("successfully startNewServer server, host=" + host + ", port=" + port);
            channelFuture.channel().closeFuture().sync();
        } catch (Exception e){
            e.printStackTrace();
        }finally {
            // Shut down all event loops to terminate all threads.
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    private void registerServer(InstanceDetails instanceDetails){
        try {
            System.out.println("channel active, will register an new Server");
            if (client == null) {
                synchronized (logger) {
                    client = CuratorFrameworkFactory.newClient(ZkServiceConf.ZK_ADDRESS, new ExponentialBackoffRetry(1000, 3));
                    client.start();
                }
            }

            //register again
            ServiceInstance<InstanceDetails> thisInstance = ServiceInstance.<InstanceDetails>builder()
                    .name(ZkServiceConf.SERVICE_NAME)
                    .payload(instanceDetails)
                    .build();

            ProtoBufInstanceSerializer<InstanceDetails> serializer = new ProtoBufInstanceSerializer(InstanceDetails.class);
            ServiceDiscovery<InstanceDetails> serviceDiscovery = ServiceDiscoveryBuilder.builder(InstanceDetails.class)
                    .client(client)
                    .basePath(ZkServiceConf.PATH)
                    .thisInstance(thisInstance)
                    .serializer(serializer)
                    .build();

            serviceDiscovery.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        InstanceDetails instanceDetails = new InstanceDetails("127.0.0.1", 12000 + new Random().nextInt(1000));
        new NettyAuthorityServer().startNewServer(instanceDetails);
    }
}
