package com.jelly;

import com.jelly.handler.AuthorityClientHandler;
import com.jelly.model.User;
import com.jelly.util.ProtoStuffSerializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jelly on 2016-8-19.
 */
public class NettyClient {
    private static Logger logger= LoggerFactory.getLogger(NettyClient.class);

    public static EventLoopGroup workerGroup;
    public static Channel channel;

    public void connect(String host,int port) throws Exception{
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
                ch.pipeline().addLast(new AuthorityClientHandler());
            }
        });

        while(true) {
            try {
                ChannelFuture f = b.connect(host, port).sync();
                channel = f.channel();
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(2000);
                continue;
            }
            break;
        }
    }

    private static void simulateClientRequireAuthority(){
        for (int i = 0; i < 1000; i++) {
            if(NettyClient.channel != null && NettyClient.channel.isOpen() && NettyClient.channel.isActive()){
                String name = "abc_def_ghi__" + i;
                User user = new User(name, i);
                byte[] bytes = ProtoStuffSerializer.serialize(user);
                logger.debug("AuthorityClientHandler write msg={}", bytes);
                NettyClient.channel.writeAndFlush(bytes);
                try {
                    Thread.sleep(9000);
                } catch (InterruptedException e) {
                }
            }else{
                logger.debug("channel is inActive, stop");
                break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            new NettyClient().connect("127.0.0.1", 5555);
            simulateClientRequireAuthority();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            logger.debug("connect finished");
            workerGroup.shutdownGracefully();
        }
    }
}
