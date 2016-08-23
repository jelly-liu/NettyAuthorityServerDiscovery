package com.jelly;

import com.jelly.handler.AuthorityServerHandler;
import com.jelly.serviceDiscovery.InstanceDetails;
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
import org.apache.log4j.Logger;

import java.util.Random;

/**
 * Created by jelly on 2016-8-19.
 */
public class NettyAuthorityServer {
    private static Logger logger = Logger.getLogger(NettyAuthorityServer.class);

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

    public static void main(String[] args) throws Exception {
        InstanceDetails instanceDetails = new InstanceDetails("127.0.0.1", 12000 + new Random().nextInt(1000));
        new NettyAuthorityServer().startNewServer(instanceDetails);
    }
}
