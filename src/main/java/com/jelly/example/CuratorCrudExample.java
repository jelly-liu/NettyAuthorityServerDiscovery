package com.jelly.example;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Created by jelly on 2016-8-22.
 */
public class CuratorCrudExample {
    private static final String add = "add";
    private static final String delete = "del";
    private static final String bye = "bye";
    private static CuratorFramework client;

    public static void main(String[] args) throws Exception {
        try {
            client = CuratorFrameworkFactory.newClient(ZkConfigExample.ZK_ADDRESS, new ExponentialBackoffRetry(1000, 3));
            client.start();

            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            while (true){
                System.out.println("please enter you command>>");
                String commandLine = in.readLine();
                if(StringUtils.isEmpty(commandLine)){
                    continue;
                }
                process(commandLine);
            }
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }

    private static void process(String commandLine){
//        client.create().withMode(CreateMode.PERSISTENT).forPath("/zk-test", "data1".getBytes(Charset.forName("UTF-8")));

        try {
            String[] segments = StringUtils.split(commandLine, " ");
            String command = segments[0].trim().toLowerCase();
            String path = segments[1];

            if (StringUtils.equals(add, command)){
                ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), path, true);
            } else if (StringUtils.equals(delete, command)) {
                ZKPaths.deleteChildren(client.getZookeeperClient().getZooKeeper(), path, true);
            } else if (StringUtils.equals(bye, command)){
                CloseableUtils.closeQuietly(client);
            }else {
                System.out.println("command no support, command=" + command);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
