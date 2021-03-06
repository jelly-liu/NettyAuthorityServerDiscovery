package com.jelly.model;

import com.jelly.serviceDiscovery.InstanceDetails;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by jelly on 2016-8-19.
 */
public class Authority {
    private InstanceDetails instanceDetails;
    private User user;
    private boolean pass;

    public Authority(User use, boolean pass, InstanceDetails instanceDetails) {
        this.user = use;
        this.pass = pass;
        this.instanceDetails = instanceDetails;
    }

    public boolean isPass() {
        return pass;
    }

    public void setPass(boolean pass) {
        this.pass = pass;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    //if number mod 5 is 0, authority pass
    public static boolean isPass(User user){
        String iStr = StringUtils.substringAfterLast(user.getName(), "__");
        int i = Integer.parseInt(iStr);
        if(i%5 == 0){
            return true;
        }else{
            return false;
        }
    }

    @Override
    public String toString() {
        String info = "";
        info += "Authority, pass=" + pass;
        if(user != null){
            info += ", user=" + user;
        }
        if(instanceDetails != null){
            info += ", instanceDetails=" + instanceDetails;
        }
        return info;
    }
}
