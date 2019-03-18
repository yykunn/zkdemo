package com.yuanyk.clock2;

import org.apache.zookeeper.KeeperException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 每个线程去zk上注册节点，注册成功的获得锁，注册失败的监听节点，节点删除后再次注册节点
 *
 */
public class Application {
    public static void main(String[] args) {
        List<Client> clients = new ArrayList<>(10);
        // 初始化10个client 建立zk连接
        for(int i=0;i<10;i++){
            clients.add(new Client("client"+i, "39.108.8.0:2181"));
        }

        // 启动10个线程去获取锁，获取锁5秒钟后释放锁，让其他线程去获取
        clients.forEach(client -> {
            new Thread(() -> {
                try {
                    client.lock();
                    TimeUnit.SECONDS.sleep(5);
                    client.unlock();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        });
    }
}
