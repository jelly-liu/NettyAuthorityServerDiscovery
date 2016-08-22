package com.jelly.serviceDiscovery;

/**
 * Created by jelly on 2016-8-22.
 */
public class InstanceDetails {
    private String host;
    private int port;

    public InstanceDetails() {
    }

    public InstanceDetails(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        return "host:" + host + ", port:" + port;
    }
}
