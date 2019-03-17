package com.yuanyk.clock;

import java.io.IOException;

/**
 * 通过在zk中创建有序临时节点实现分布式锁
 *
 * 每个服务在zk的/LOCK节点下创建一个有序临时节点，
 * 序号最小的获得锁，序号不是最小的 对 比自己的序号小1的节点 注册监听，当比自己序号小1的节点删除时，自己获得锁
 */
public class Application {
    public static void main(String[] args) {
        Client[] clients = new Client[10];
        for(int i=0;i<clients.length;i++){
            Client client = new Client("client"+i, "39.108.8.0:2181");
            clients[i] = client;
        }
        for(Client client:clients){
            new Thread(() -> {
                if(client.lock()){
                    System.out.println(client.getName()+" get this lock ,doing......");
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                client.unLock();
            }).start();
        }

    }
}
