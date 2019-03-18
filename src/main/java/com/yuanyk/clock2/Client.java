package com.yuanyk.clock2;

import com.yuanyk.clock.DeleteWatch;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class Client implements Watcher {
    private String name;
    private ZooKeeper zooKeeper;
    private CountDownLatch latch = new CountDownLatch(1);
    private static final String PATH = "/LOCK2";
    public Client(String name,String connectStr){
        this.name = name;
        try {
            zooKeeper = new ZooKeeper(connectStr, 5000,this);
            latch.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void lock() throws KeeperException, InterruptedException {
        try{
            String result = zooKeeper.create(PATH, name.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println(name+" get lock");
        }catch (KeeperException e){
            // 如果节点已存在，说明锁被其他client抢到，对节点注册监听，当节点删除时再次获取锁
            System.out.println(name+" donot get lock,wating...");
            CountDownLatch deleteWatch = new CountDownLatch(1);
            zooKeeper.exists(PATH, new DeleteWatch(deleteWatch));
            deleteWatch.await();
            lock();
        }
    }

    public void unlock() throws KeeperException, InterruptedException {
        zooKeeper.delete(PATH, -1);
        System.out.println(name+" un lock");
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getState()== Event.KeeperState.SyncConnected){
            latch.countDown();
        }
    }
}
