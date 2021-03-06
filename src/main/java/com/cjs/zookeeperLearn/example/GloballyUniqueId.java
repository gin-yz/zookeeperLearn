/*
* 为数据库设置分布式全局自增ｉｄ
*
* */


package com.cjs.zookeeperLearn.example;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import com.cjs.zookeeperLearn.watcher.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class GloballyUniqueId implements Watcher {
    //  zk的连接串
    String IP = "hadoop2:2181,hadoop1:2181,hadoop2:2181"; //连接集群
    //  计数器对象
    CountDownLatch countDownLatch = new CountDownLatch(1);
    //  用户生成序号的节点
    String defaultPath = "/uniqueID/idIs";
    //  连接对象
    ZooKeeper zooKeeper;

    @Override
    public void process(WatchedEvent event) {
        try {
            // 捕获事件状态
            if (event.getType() == Event.EventType.None) {
                if (event.getState() == KeeperState.SyncConnected) {
                    System.out.println("连接成功");
                    countDownLatch.countDown();
                } else if (event.getState() == KeeperState.Disconnected) {
                    System.out.println("连接断开!");
                } else if (event.getState() == KeeperState.Expired) {
                    System.out.println("连接超时!");
                    // 超时后服务器端已经将连接释放，需要重新连接服务器端
                    zooKeeper = new ZooKeeper(IP, 6000,
                            new ZKConnectionWatcher());
                } else if (event.getState() == KeeperState.AuthFailed) {
                    System.out.println("验证失败!");
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // 构造方法
    public GloballyUniqueId() {
        try {
            //打开连接
            zooKeeper = new ZooKeeper(IP, 5000, this);
            // 阻塞线程，等待连接的创建成功
            countDownLatch.await();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    // 生成id的方法
    public String getUniqueId() {
        String path = "";
        try {
            //创建临时有序节点
            path = zooKeeper.create(defaultPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        // /uniqueId0000000001uniqueId
        return path.substring(14);
    }

    public static void main(String[] args) throws InterruptedException {
        GloballyUniqueId globallyUniqueId = new GloballyUniqueId();
//        for (int i = 1; i <= 5; i++) {
//            String id = globallyUniqueId.getUniqueId();
//            System.out.println(id);
//        }

        IntStream.range(0,20).mapToObj(new IntFunction<Thread>() {
            @Override
            public Thread apply(int value) {
                return new Thread(()->{
                    String id = globallyUniqueId.getUniqueId();
                    System.out.println(id);
                });
            }
        }).forEach(Thread::start);
        TimeUnit.SECONDS.sleep(10);
    }

}