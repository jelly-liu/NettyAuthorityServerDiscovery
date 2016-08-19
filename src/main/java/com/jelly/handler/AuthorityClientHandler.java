package com.jelly.handler;


import com.jelly.NettyClient;
import com.jelly.model.Authority;
import com.jelly.util.ProtoStuffSerializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jelly on 2016-8-19.
 */
public class AuthorityClientHandler extends ChannelInboundHandlerAdapter {
    private Logger logger= LoggerFactory.getLogger(AuthorityServerHandler.class);

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.debug("AuthorityClientHandler channel already active");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        byte[] bytes = (byte[]) msg;
        Authority authority = ProtoStuffSerializer.deserialize(bytes, Authority.class);
        //notice, you should use your thread pool to process data
        logger.debug("receive data, authority={}", authority);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        logger.debug("exceptionCaught");
        cause.printStackTrace();
        ctx.close();
        NettyClient.channel = null;
    }
}
