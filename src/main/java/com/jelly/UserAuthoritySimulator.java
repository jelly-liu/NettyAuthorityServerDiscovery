package com.jelly;

import com.jelly.model.User;
import com.jelly.serviceDiscovery.InstanceDetails;
import com.jelly.util.ChannelManager;
import com.jelly.util.KeyUtil;
import com.jelly.util.ProtoStuffSerializer;
import io.netty.channel.Channel;

/**
 * Created by jelly on 2016-8-24.
 */
public class UserAuthoritySimulator {
    /**
     * for an client, lots of user need do permissions validation
     * and client will choose an active server to do authority
     * then print out result
     */
    public static void simulateClientRequireAuthorityToServer(){
        new Thread(()->{
            ChannelManager.ChannelInstance channelInstance = null;
            for (int i = 0; i < 1000; i++) {
                channelInstance = NettyAuthorityClient.channelManager.getRoundRobinChannel();
                if(channelInstance != null){
                    Channel channel = channelInstance.channel;
                    InstanceDetails instanceDetails = channelInstance.instanceDetails;
                    if(channel != null && channel.isOpen() && channel.isActive()){
                        String name = instanceDetails.toString() + "__" + channelInstance.channelName + "__abc_def_ghi__" + i;
                        User user = new User(name, i);
                        byte[] bytes = ProtoStuffSerializer.serialize(user);
                        System.out.println("AuthorityClientHandler write msg=" + bytes);
                        channel.writeAndFlush(bytes);
                    }else{
                        if(instanceDetails != null){
                            NettyAuthorityClient.channelManager.removeChannel(KeyUtil.toMD5(instanceDetails.toString()));
                        }
                    }
                }
                try {
                    Thread.sleep(6000);
                } catch (InterruptedException e) {
                }
            }
        }).start();
    }
}
