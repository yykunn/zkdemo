package com.yuanyk.clock;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 一个client模拟一个服务
 */
public class Client implements Watcher {
    private String name;
    private ZooKeeper zooKeeper;
    private CountDownLatch latch = new CountDownLatch(1);
    private String path = "/LOCK";
    private boolean locked = false;
    private String lockPath;

    public String getName(){
        return name;
    }

    private Client() {
    }

    public Client(String name, String zkConnectStr) {
        this.name = name;
        try {
            zooKeeper = new ZooKeeper(zkConnectStr, 5000, this);
            latch.await();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            latch.countDown();
        }
    }

    public boolean lock() {
        try {
            String result = zooKeeper.create(path+"/", name.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            lockPath = result;
            List<String> locks = zooKeeper.getChildren(path, true);
            SortedSet<String> sets = new TreeSet<>();
            locks.forEach(s -> sets.add(path +"/"+ s));
            if (sets.first().equals(result)) {
                System.out.println(name + " get lock id:"+lockPath);
                locked = true;
                return true;
            }
            SortedSet<String> lessSet = sets.headSet(result);
            String less = lessSet.last();
            CountDownLatch countDownLatch = new CountDownLatch(1);
            zooKeeper.exists(less, new DeleteWatch(countDownLatch));
            countDownLatch.await();
            locked = true;
            System.out.println(name+" get lock id:"+lockPath);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean unLock() {
        if(locked){
            try {
                zooKeeper.delete(lockPath, -1);
                return true;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }else{
            System.out.println(name+" does not get lock can not unlock");
            return false;
        }
    }

}
