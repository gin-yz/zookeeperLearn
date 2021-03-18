package com.cjs.zookeeperLearn.example;

import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

public class TicketSeller {

    private void sell(){
        System.out.println("售票开始");
        // 线程随机休眠数毫秒，模拟现实中的费时操作
        int sleepMillis = 500;
        try {
            //代表复杂逻辑执行了一段时间
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("售票结束");
    }

    public void sellTicketWithLock() throws Exception {
        MyLock lock = new MyLock();
        // 获取锁
        lock.acquireLock();
        sell();
        TimeUnit.SECONDS.sleep(1);
        //释放锁
        lock.releaseLock();
    }
    public static void main(String[] args) throws Exception {
        TicketSeller ticketSeller = new TicketSeller();
//        for(int i=0;i<10;i++){
//            ticketSeller.sellTicketWithLock();
//        }

        IntStream.range(0,500).mapToObj(new IntFunction<Thread>() {
            @Override
            public Thread apply(int value) {
                return new Thread(()->{
                    try {
                        ticketSeller.sellTicketWithLock();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        }).forEach(Thread::start);
    }
}
