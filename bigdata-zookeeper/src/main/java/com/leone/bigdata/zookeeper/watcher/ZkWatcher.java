package com.leone.bigdata.zookeeper.watcher;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * 一次性的触发器（one-time trigger)
 * <p>
 * 可以注册watcher的方法：getData、exists、getChildren。
 * <p>
 * 可以触发watcher的方法：create、delete、setData。连接断开的情况下触发的watcher会丢失。
 * <p>
 * 一个Watcher实例是一个回调函数，被回调一次后就被移除了。如果还需要关注数据的变化，需要再次注册watcher。
 * <p>
 * New ZooKeeper时注册的watcher叫default watcher，它不是一次性的，只对client的连接状态变化作出反应。
 *
 * <p>Watch的整体流程为，客户端先向ZooKeeper服务端成功注册想要监听的节点状态，同时客户端本地会存储该监听器相关的信息在WatchManager中，
 * 当ZooKeeper服务端监听的数据状态发生变化时，ZooKeeper就会主动通知发送相应事件信息给相关会话客户端，客户端就会在本地响应式的回调相关Watcher的Handler
 * <p>
 * 1.Watch是一次性的，每次都需要重新注册，并且客户端在会话异常结束时不会收到任何通知，而快速重连接时仍不影响接收通知。
 * 2.Watch的回调执行都是顺序执行的，并且客户端在没有收到关注数据的变化事件通知之前是不会看到最新的数据，另外需要注意不要在Watch回调逻辑中阻塞整个客户端的Watch回调Watch是轻量级的，WatchEvent是最小的通信单元，结构上只包含通知状态、事件类型和节点路径。
 * 3.ZooKeeper服务端只会通知客户端发生了什么，并不会告诉具体内容。
 * <p>
 * x
 *
 * @author leone
 * @since 2019-01-28
 **/
public class ZkWatcher implements Watcher {

    private final static Logger logger = LoggerFactory.getLogger(ZkWatcher.class);

    private static final String host = "ip:2181";

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static ZooKeeper zooKeeper;

    public static void main(String[] args) throws Exception {
        // 注册一个监听事件
        zooKeeper = new ZooKeeper(host, 5000, new ZkWatcher());
        // Client session timed out, have not heard from server in 9022ms for sessionid 0x0
        countDownLatch.await();

        create("/test", "test");

        Thread.sleep(3000);

        exists("/a4", true);

        Thread.sleep(3000);

        setData("/test", "123");

        Thread.sleep(3000);

        getData("/test");

        Thread.sleep(3000);

        delete("/test");

    }

    /**
     * 监听事件的回调方法
     *
     * @param watchedEvent
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        // 连接状态
        Event.KeeperState keeperState = watchedEvent.getState();

        // 事件类型
        Event.EventType eventType = watchedEvent.getType();

        // 受影响的path
        String path = watchedEvent.getPath();

        /*
         * KeeperState.Disconnected     连接不上
         * KeeperState.SyncConnected    连接上了
         * KeeperState.AuthFailed       认证失败
         * KeeperState.Expired          过期
         */
        if (Event.KeeperState.SyncConnected == keeperState) {
            /*
             * EventType.NodeCreated                节点创建
             * EventType.NodeDataChanged            节点的数据发生变更
             * EventType.NodeChildrenChanged        子节点发生变更
             * EventType.NodeDeleted                节点被删除
             */
            if (eventType == Event.EventType.None) {
                logger.info("keeperState {} connection zookeeper success...", keeperState);
                countDownLatch.countDown();
            } else if (eventType == Event.EventType.NodeCreated) {
                logger.info("watcher NodeCreated: {}", watchedEvent);
            } else if (eventType == Event.EventType.NodeDataChanged) {
                logger.info("watcher NodeDataChanged: {}", watchedEvent);
            } else if (eventType == Event.EventType.NodeDeleted) {
                logger.info("watcher NodeDeleted: {}", watchedEvent);
            } else if (eventType == Event.EventType.DataWatchRemoved) {
                logger.info("watcher DataWatchRemoved: {}", watchedEvent);
            } else if (eventType == Event.EventType.ChildWatchRemoved) {
                logger.info("watcher ChildWatchRemoved: {}", watchedEvent);
            } else if (eventType == Event.EventType.NodeChildrenChanged) {
                logger.info("watcher NodeChildrenChanged: {}", watchedEvent);
            }
        } else if (Event.KeeperState.Disconnected == keeperState) {
            logger.info("Disconnected...");
        } else if (Event.KeeperState.Expired == keeperState) {
            logger.info("Expired...");
        } else if (Event.KeeperState.AuthFailed == keeperState) {
            logger.info("AuthFailed...");
        } else if (Event.KeeperState.Closed == keeperState) {
            logger.info("Closed...");
        } else if (Event.KeeperState.ConnectedReadOnly == keeperState) {
            logger.info("ConnectedReadOnly...");
        } else if (Event.KeeperState.SaslAuthenticated == keeperState) {
            logger.info("SaslAuthenticated...");
        }
    }


    /**
     * PERSISTENT：  永久节点
     * EPHEMERAL：   临时节点
     * PERSISTENT_SEQUENTIAL：   永久节点、序列化
     * EPHEMERAL_SEQUENTIAL：    临时节点、序列化
     *
     * @param path
     * @param data
     * @throws Exception
     */
    public static void create(final String path, String data) throws Exception {
        // 重新注册监听
        zooKeeper.exists(path, true);
        // 1.节点的路径 2.节点数据 3:节点的权限 4:节点的类型
        String s = zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        logger.info("create: {}", s);
    }

    /**
     * @param path
     * @throws Exception
     */
    public static void delete(final String path) throws Exception {
        // 重新注册监听
        zooKeeper.getChildren(path, true);
        // 1.节点的路径 2.版本 -1 表示所有版本
        zooKeeper.delete(path, -1);
        logger.info("delete: {}", path);
    }


    /**
     * @param path
     * @param data
     * @throws Exception
     */
    public static void setData(final String path, final String data) throws Exception {
        // 重新注册监听
        zooKeeper.getData(path, true, null);
        // 1.节点的路径 2.数据 3.版本
        zooKeeper.setData(path, data.getBytes(), -1);
        logger.info("setData: {} {}", path, data);
    }

    /**
     * @param path
     * @param needWatch
     * @throws Exception
     */
    public static void exists(final String path, boolean needWatch) throws Exception {
        // 1.节点的路径 2.是否需要监听
        Stat stat = zooKeeper.exists(path, System.out::println);
        logger.info("exists: {} {}", path, stat);
    }

    /**
     * @param path
     * @throws Exception
     */
    public static void getData(final String path) throws Exception {
        byte[] data = zooKeeper.getData(path, false, null);
        logger.info("getData: {}", new String(data));
    }

}
