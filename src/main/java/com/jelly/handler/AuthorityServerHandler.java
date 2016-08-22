package com.jelly.handler;

import com.jelly.model.Authority;
import com.jelly.model.User;
import com.jelly.util.ProtoStuffSerializer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jelly on 2016-8-19.
 */
public class AuthorityServerHandler extends ChannelInboundHandlerAdapter {
    private Logger logger= LoggerFactory.getLogger(AuthorityServerHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        byte[] bytes = (byte[]) msg;
        User user = ProtoStuffSerializer.deserialize(bytes, User.class);
        //notice, you should use your thread pool to process data
        System.out.println("receive data, object=" + user);

        Authority authority = new Authority(user, Authority.isPass(user));
        bytes = ProtoStuffSerializer.serialize(authority);
        ctx.write(bytes);
        System.out.println("send data, object=" + authority);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
