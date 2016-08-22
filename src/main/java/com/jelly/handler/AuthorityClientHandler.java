package com.jelly.handler;


import com.jelly.NettyClient;
import com.jelly.model.Authority;
import com.jelly.serviceDiscovery.InstanceDetails;
import com.jelly.util.KeyUtil;
import com.jelly.util.ProtoStuffSerializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;

/**
 * Created by jelly on 2016-8-19.
 */
public class AuthorityClientHandler extends ChannelInboundHandlerAdapter {
    private Logger logger= LoggerFactory.getLogger(AuthorityServerHandler.class);

    private InstanceDetails instanceDetails;

    public AuthorityClientHandler(InstanceDetails instanceDetails){
        this.instanceDetails = instanceDetails;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.out.println("AuthorityClientHandler channel already active");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        byte[] bytes = (byte[]) msg;
        Authority authority = ProtoStuffSerializer.deserialize(bytes, Authority.class);
        //notice, you should use your thread pool to process data
        System.out.println("receive data, authority=" + authority);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        System.out.println("exceptionCaught, will close channel, and remove key=" + KeyUtil.toMD5(instanceDetails.toString()));
        cause.printStackTrace();
        ctx.close();
        ctx.channel().close();
        NettyClient.serverMap.remove(KeyUtil.toMD5(instanceDetails.toString()));
    }
}
