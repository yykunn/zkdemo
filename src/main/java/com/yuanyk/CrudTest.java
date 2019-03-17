package com.yuanyk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 增删改查,注册监听器，获取子节点测试
 */
public class CrudTest implements Watcher {

    //private static final String CONECTSTRIG = "192.168.213.200:2181";
    private static final String CONECTSTRIG = "39.108.8.0:2181";

    // 连接成功时减一
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private ZooKeeper zooKeeper;

    public ZooKeeper init() {
        try {
            zooKeeper = new ZooKeeper(CONECTSTRIG, 5000, this);
            // 等待连接成功
            countDownLatch.await();
            System.out.println("zookeeper state:" + zooKeeper.getState());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return zooKeeper;
    }

    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            countDownLatch.countDown();
        }
    }

    /**
     * 获取节点数据
     *
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     * @throws IOException
     */
    public String get(String path) throws KeeperException, InterruptedException, IOException {
        byte[] bytes = zooKeeper.getData(path, true, null);
        return new String(bytes);
    }

    /**
     * 删除节点
     *
     * @param path
     * @throws KeeperException
     * @throws InterruptedException
     * @throws IOException
     */
    public void delete(String path) throws KeeperException, InterruptedException, IOException {
        zooKeeper.delete(path, -1);
    }

    /**
     * 更新节点数据
     *
     * @param path
     * @param data
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void update(String path, String data) throws IOException, InterruptedException, KeeperException {
        Stat stat = zooKeeper.setData(path, data.getBytes(), -1);
        System.out.println("更新节点成功：" + stat);
    }

    /**
     * 新增节点
     *
     * @param path
     * @param data
     * @throws KeeperException
     * @throws InterruptedException
     * @throws IOException
     */
    public void create(String path, String data) throws KeeperException, InterruptedException, IOException {
        String result = zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("新增节点成功：" + result);
    }

    /**
     * 给节点添加监测器
     *
     * @param path
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void addDeleteWatch(String path) throws IOException, InterruptedException, KeeperException {
        zooKeeper.exists(path, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                    System.out.println("node " + watchedEvent.getPath() + " has be deleted");
                }
            }
        });
    }

    /**
     * 获取节点所有子节点的路径
     *
     * @param path
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    public List<String> list(String path) throws IOException, InterruptedException, KeeperException {
        return zooKeeper.getChildren(path, true);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        CrudTest crudTest = new CrudTest();
        crudTest.init();
        // 获取跟路径下的所有节点
        List<String> rootChildern = crudTest.list("/");
        System.out.println("根路径节点列表：");
        rootChildern.forEach(s -> System.out.println(s));

        String path = "/yuanyk";
        // 新增节点
        crudTest.create(path, "优衣库");
        System.out.println("新增节点后，根路径列表：");
        crudTest.list("/").forEach(s -> System.out.println(s));
        System.out.println();

        // 获取节点数据
        String data = crudTest.get(path);
        System.out.println("get data:" + data);

        // 给节点添加删除节点到监听器
        crudTest.addDeleteWatch(path);

        // 更新节点数据
        crudTest.update(path, "英语课");
        System.out.println("获取到更新后的数据：" + crudTest.get(path));

        // 删除节点，测试监听器
        crudTest.delete(path);

        System.out.println("删除节点后，根路径列表：");
        crudTest.list("/").forEach(s -> System.out.println(s));
    }
}

