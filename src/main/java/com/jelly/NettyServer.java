package com.jelly;

import com.jelly.handler.AuthorityServerHandler;
import com.jelly.serviceDiscovery.InstanceDetails;
import com.jelly.serviceDiscovery.ServiceUtil;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Created by jelly on 2016-8-19.
 */
public class NettyServer {
    private static Logger logger = Logger.getLogger(NettyServer.class);

    private static CuratorFramework client = null;

    public void start(String host, int port) throws Exception {
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
                    ch.pipeline().addLast(new AuthorityServerHandler());
                }
            });

            // Start the server.
            ChannelFuture channelFuture = serverBootstrap.bind(host, port).sync();
            //checkAndRegister server
            registerServer(host, port);
            // Wait until the server socket is closed.
            channelFuture.channel().closeFuture().sync();
        } finally {
            // Shut down all event loops to terminate all threads.
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    private static void registerServer(String host, int port){
        if(client == null){
            synchronized (logger){
                client = CuratorFrameworkFactory.newClient(ZkServiceConf.ZK_ADDRESS, new ExponentialBackoffRetry(1000, 3));
                client.start();
            }
        }
        ServiceUtil.checkAndRegister(client, new InstanceDetails(host, port));
    }

    public static void main(String[] args) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while (true){
            System.out.print("\nPlease enter an number as port, [1 <= port <= 100], --->");
            String portStr = in.readLine();
            if(StringUtils.isEmpty(portStr)){
                continue;
            }

            int port = -1;
            try{
                port = Integer.parseInt(portStr);
                if(!(1 <= port && port <=100)){
                    continue;
                }
            }catch (Exception e){
                System.out.print("\nPlease Enter An Number>>>>>>");
                e.printStackTrace();
                continue;
            }

            int finalPort = port;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        new NettyServer().start("127.0.0.1", 12000 + finalPort);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }
}
