package com.cjs.zookeeperLearn.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.locks.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CuratorLock {

    String IP = "hadoop1:2181,hadoop2:2181,hadoop3:2181";
    CuratorFramework client;
    CuratorFramework client2;

    @Before
    public void before() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory
                .builder()
                .connectString(IP)
                .sessionTimeoutMs(10000)
                .retryPolicy(retryPolicy)
                .build();
        client2 = CuratorFrameworkFactory
                .builder()
                .connectString(IP)
                .sessionTimeoutMs(10000)
                .retryPolicy(retryPolicy)
                .build();

        client.start();
        client2.start();
    }

    @After
    public void after() {
        client.close();
        client2.close();
    }


    @Test
    public void lock1() throws Exception {
        // 排他锁
        // arg1:连接对象
        // arg2:节点路径
        InterProcessLock interProcessLock = new InterProcessMutex(client, "/lock1");

        Stream<Thread> threadStream = IntStream.range(0, 100).mapToObj(new IntFunction<Thread>() {
            @Override
            public Thread apply(int value) {

                return new Thread(() -> {

                    // 获取锁
                    try {
                        System.out.println("等待获取锁对象!");
                        interProcessLock.acquire(10, TimeUnit.SECONDS);
                        TimeUnit.SECONDS.sleep(2);
                        interProcessLock.release();
                        System.out.println("等待释放锁!");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                });
            }
        });
        threadStream.forEach(Thread::start);


    }

    @Test
    public void lock2() throws Exception {
        // 读写锁
        InterProcessReadWriteLock interProcessReadWriteLock = new InterProcessReadWriteLock(client, "/lock1");
        // 获取读锁对象
        InterProcessLock interProcessLock = interProcessReadWriteLock.readLock();
        System.out.println("等待获取锁对象!");
        // 获取锁
        interProcessLock.acquire();
        for (int i = 1; i <= 10; i++) {
            Thread.sleep(3000);
            System.out.println(i);
        }
        // 释放锁
        interProcessLock.release();
        System.out.println("等待释放锁!");
    }

    @Test
    public void lock3() throws Exception {
        // 读写锁
        InterProcessReadWriteLock interProcessReadWriteLock = new InterProcessReadWriteLock(client, "/lock1");
        // 获取写锁对象
        InterProcessLock interProcessLock = interProcessReadWriteLock.writeLock();
        System.out.println("等待获取锁对象!");
        // 获取锁
        interProcessLock.acquire();
        for (int i = 1; i <= 10; i++) {
            Thread.sleep(3000);
            System.out.println(i);
        }
        // 释放锁
        interProcessLock.release();
        System.out.println("等待释放锁!");
    }

    @Test
    public void witerReadDemo() throws Exception {
        InterProcessReadWriteLock interProcessReadWriteLock = new InterProcessReadWriteLock(client, "/lock1");
        InterProcessLock readLock = interProcessReadWriteLock.readLock();
        InterProcessLock writeLock = interProcessReadWriteLock.writeLock();
        CountDownLatch countDownLatch = new CountDownLatch(40);

        IntStream.range(0, 20).mapToObj(new IntFunction<Thread>() {
            @Override
            public Thread apply(int value) {
                return new Thread(() -> {
                    try {
                        readLock.acquire();
                        System.out.println("read lock require,no:" + value);
                        TimeUnit.SECONDS.sleep(3);

                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            readLock.release();
                            System.out.println("read lock release,no:" + value);
                            countDownLatch.countDown();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                }, String.valueOf(value));
            }
        }).forEach(Thread::start);

        IntStream.range(0, 20).mapToObj(new IntFunction<Thread>() {
            @Override
            public Thread apply(int value) {
                return new Thread(() -> {
                    try {
                        writeLock.acquire();
                        System.out.println("write lock require,no:" + value);
                        TimeUnit.SECONDS.sleep(1);

                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            writeLock.release();
                            System.out.println("write lock release,no:" + value);
                            countDownLatch.countDown();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                }, String.valueOf(value));
            }
        }).forEach(Thread::start);

        //主进程在这等着，不然直接退出了
        countDownLatch.await();
    }

    @Test
    public void sharedReentrantReadWriteLock() throws Exception {
        String lockPath = "/lock1";
        // 创建共享可重入读写锁
        final InterProcessReadWriteLock locl1 = new InterProcessReadWriteLock(client, lockPath);
        // 获取读写锁(使用 InterProcessMutex 实现, 所以是可以重入的)
        final InterProcessLock readLock = locl1.readLock();

        final CountDownLatch countDownLatch = new CountDownLatch(2);

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    readLock.acquire();
                    System.out.println("1获取读锁===============");
                    // 测试锁重入
                    readLock.acquire();
                    System.out.println("1再次获取读锁===============");
                    Thread.sleep(5 * 1000);
                    readLock.release();
                    System.out.println("1释放读锁===============");
                    readLock.release();
                    System.out.println("1再次释放读锁===============");

                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    readLock.acquire();
                    System.out.println("1获取读锁===============");
                    // 测试锁重入
                    readLock.acquire();
                    System.out.println("1再次获取读锁===============");
                    Thread.sleep(5 * 1000);
                    readLock.release();
                    System.out.println("1释放读锁===============");
                    readLock.release();
                    System.out.println("1再次释放读锁===============");

                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        countDownLatch.await();

    }

    @Test
    public void semaphoreDemo() throws InterruptedException {
        InterProcessSemaphoreV2 semaphore = new InterProcessSemaphoreV2(client, "/lock1", 5);

        CountDownLatch countDownLatch = new CountDownLatch(30);

        IntStream.range(0, 30).mapToObj(new IntFunction<Thread>() {
            @Override
            public Thread apply(int value) {
                return new Thread(() -> {
                    try {
                        Lease lease = semaphore.acquire();
                        System.out.println("acquire the lock,no:" + value);
                        TimeUnit.SECONDS.sleep(3);
                        System.out.println("realse the lock,no:" + value);
                        semaphore.returnLease(lease);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        countDownLatch.countDown();
                    }
                }, String.valueOf(value));
            }
        }).forEach(Thread::start);

        countDownLatch.await();
    }


    //多重共享锁
    @Test
    public void multiLock() throws Exception {
        String lockPath = "/lock1";
        // 可重入锁
        final InterProcessLock interProcessLock1 = new InterProcessMutex(client, lockPath);
        // 不可重入锁
        final InterProcessLock interProcessLock2 = new InterProcessSemaphoreMutex(client2, lockPath);
        // 创建多重锁对象
        final InterProcessLock lock = new InterProcessMultiLock(Arrays.asList(interProcessLock1, interProcessLock2));

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 获取锁对象
                try {
                    // 获取参数集合中的所有锁
                    lock.acquire();
                    // 因为存在一个不可重入锁, 所以整个 InterProcessMultiLock 不可重入
                    System.out.println(lock.acquire(2, TimeUnit.SECONDS));
                    // interProcessLock1 是可重入锁, 所以可以继续获取锁
                    System.out.println(interProcessLock1.acquire(2, TimeUnit.SECONDS));
                    // interProcessLock2 是不可重入锁, 所以获取锁失败
                    System.out.println(interProcessLock2.acquire(2, TimeUnit.SECONDS));

                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();

        countDownLatch.await();
    }
}