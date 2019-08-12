package com.leone.bigdata.zookeeper;

import com.sun.org.apache.xml.internal.resolver.readers.ExtendedXMLCatalogReader;
import org.apache.curator.utils.ZookeeperFactory;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.WatcherEvent;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * ZooKeeper -server host:port cmd args
 * stat path [watch]
 * set path data [version]
 * ls path [watch]
 * delquota [-n|-b] path
 * ls2 path [watch]
 * setAcl path acl
 * setquota -n|-b val path
 * history
 * redo cmdno
 * printwatches on|off
 * delete path [version]
 * sync path
 * listquota path
 * rmr path
 * get path [watch]
 * create [-s] [-e] path data acl
 * addauth scheme auth
 * quit
 * getAcl path
 * close
 * connect host:port
 *
 * @author leone
 * @since 2018-06-16
 **/
public class ZkClient {

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private final static Logger logger = LoggerFactory.getLogger(ZkClient.class);

    private final static String CONNECT_STRING = "ip:2181";

    private final static int SESSION_TIMEOUT = 5000;

    private static ZooKeeper zooKeeper;

    static {
        try {
            zooKeeper = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, (event) -> {
                Watcher.Event.KeeperState state = event.getState();
                if (state == Watcher.Event.KeeperState.SyncConnected) {
                    logger.info("create session successful...");
                    countDownLatch.countDown();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        countDownLatch.await();
        String path = "/hello";
        String childrenPath1 = "/hello/a1";
        String childrenPath2 = "/hello/a2";

        create(zooKeeper, path, "000");

        Thread.sleep(3000);

        exists(zooKeeper, path);

        Thread.sleep(3000);

        getData(zooKeeper, path);

        Thread.sleep(3000);

        setData(zooKeeper, path, "111");

        Thread.sleep(3000);

        create(zooKeeper, childrenPath1, "001");
        create(zooKeeper, childrenPath2, "002");


        Thread.sleep(3000);

        getData(zooKeeper, path);

        Thread.sleep(3000);

        getChildren(zooKeeper, path);

        Thread.sleep(3000);

        delete(zooKeeper, childrenPath1);
        delete(zooKeeper, childrenPath2);

        Thread.sleep(3000);

        delete(zooKeeper, path);
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
    public static void create(ZooKeeper zookeeper, String path, String data) throws Exception {
        // 1.节点的路径 2.节点数据 3:节点的权限 4:节点的类型
        String s = zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        logger.info("create: {}", s);
    }

    /**
     * @param zooKeeper
     * @param path
     * @throws Exception
     */
    public static void exists(ZooKeeper zooKeeper, String path) throws Exception {
        // 1.节点的路径 2.是否需要监听
        Stat stat = zooKeeper.exists(path, false);
        logger.info("exists: {} {}", path, stat);
    }

    /**
     * @param zooKeeper
     * @param path
     * @throws Exception
     */
    public static void getData(ZooKeeper zooKeeper, String path) throws Exception {
        byte[] data = zooKeeper.getData(path, false, null);
        logger.info("getData: {}", new String(data));
    }

    /**
     * @param path
     * @param data
     * @throws Exception
     */
    public static void setData(ZooKeeper zooKeeper, String path, String data) throws Exception {
        // 1.节点的路径 2.数据 3.版本
        zooKeeper.setData(path, data.getBytes(), -1);
        logger.info("setData: {} {}", path, data);
    }

    /**
     * @param zooKeeper
     * @param path
     * @throws Exception
     */
    public static void delete(ZooKeeper zooKeeper, String path) throws Exception {
        // 1.节点的路径 2.版本 -1 表示所有版本
        zooKeeper.delete(path, -1);
        logger.info("delete: {}", path);
    }


    /**
     * 获取子节点
     *
     * @param zooKeeper
     * @param path
     * @throws Exception
     */
    public static void getChildren(ZooKeeper zooKeeper, String path) throws Exception {
        // 1.节点的路径 2.版本 -1 表示所有版本
        List<String> children = zooKeeper.getChildren(path, false);
        children.forEach(e -> {
            logger.info("getChildren: {}", e);
        });
    }

}
