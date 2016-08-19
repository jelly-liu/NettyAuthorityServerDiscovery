package com.jelly.model;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by jelly on 2016-8-19.
 */
public class Authority {
    private User user;
    private boolean pass;

    public Authority(User use, boolean pass) {
        this.user = use;
        this.pass = pass;
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
        return info;
    }
}
