package com.jelly.serviceDiscovery;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.CreateMode;

import java.nio.charset.Charset;

/**
 * Created by jelly on 2016-8-22.
 */
public class ZkCrudExample {
    public static void main(String[] args) throws Exception {
        CuratorFramework client = null;
        try {
            client = CuratorFrameworkFactory.newClient(ZkConfig.ZK_ADDRESS, new ExponentialBackoffRetry(1000, 3));
            client.start();

            client.create().withMode(CreateMode.PERSISTENT).forPath("/zk-test", "data1".getBytes(Charset.forName("UTF-8")));
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }
}
