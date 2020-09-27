package com.cjs.zookeeperLearn.test1;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

public class Demo1 {
    @Test
    public void createZnode() throws Exception {
        //创建重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
        //获取客户端对象
        String connectionStr = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionStr, 8000, 8000, retryPolicy);
        //开启客户端
        client.start();
        //创建节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello3", "word3".getBytes());
        //关闭客户端
        client.close();
    }
}
