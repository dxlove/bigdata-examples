package com.leone.bigdata.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final static String ZK_URL = "xxx.xxx.xxx.xxx:2181";

    private final static int TIME_OUT = 5000;

    private static ZooKeeper zkClient = null;

    @Before
    public void init() throws Exception {
        zkClient = new ZooKeeper(ZK_URL, TIME_OUT, watchedEvent -> logger.info("{}", watchedEvent));
    }


    /**
     * @throws Exception
     */
    public static void demo1() throws Exception {
        //初始化zk
        ZooKeeper zooKeeper = new ZooKeeper(ZK_URL, TIME_OUT, (WatchedEvent watchedEvent) -> {
            Watcher.Event.KeeperState state = watchedEvent.getState();
            Watcher.Event.EventType type = watchedEvent.getType();
            if (Watcher.Event.KeeperState.SyncConnected == state) {
                if (Watcher.Event.EventType.None == type) {
                    //调用此方法测计数减一
                    countDownLatch.countDown();
                }
            }
        });
        //阻碍当前线程进行,除非计数归零
        countDownLatch.await();
        try {
            //创建持久化节点
            zooKeeper.create("/com.andy", "你好".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //获取节点数据
            byte[] data = zooKeeper.getData("/com.andy", false, null);
            System.out.println(new String(data));
            //修改节点数据
            zooKeeper.setData("/com.andy", "james".getBytes(), 0);
            //删除节点数据
            zooKeeper.delete("/com.andy", -1);
            //创建临时节点 异步创建
            zooKeeper.create("/com.andy", "tmp".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, new AsyncCallback.StringCallback() {
                public void processResult(int i, String s, Object o, String s1) {
                    System.out.println(o);
                    System.out.println(i);
                    System.out.println(s1);
                    System.out.println(s);
                }
            }, "a");
            //获取临时节点数据
            byte[] tmp = zooKeeper.getData("/com.andy", false, null);
            System.out.println(new String(tmp));
            //验证节点是否存在
            Stat exists = zooKeeper.exists("/com.andy", false);
            System.out.println(exists);
        } catch (Exception e) {
            e.printStackTrace();
        }
        zooKeeper.close();
    }


    /**
     * 设置值
     *
     * @throws Exception
     */
    @Test
    public void testSetData() throws Exception {
        zkClient.setData("/hello", "world".getBytes(), -1);
        byte[] data = zkClient.getData("/hello", false, null);
        logger.info(new String(data));
    }

    /**
     * 创建节点
     *
     * @throws Exception
     */
    @Test
    public void testCreate() throws Exception {
        // 参数1：要创建的节点的路径 参数2：节点数据 参数3：节点的权限 参数4：节点的类型
        zkClient.create("/hello", "jack".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }


    /**
     * 测试某节点是否存在
     *
     * @throws Exception
     */
    @Test
    public void testExists() throws Exception {
        Stat stat = zkClient.exists("/eclipse", false);
        logger.info(stat == null ? "not exist" : "exist");
    }

    /**
     * 获取子节点
     *
     * @throws Exception
     */
    @Test
    public void testGetChild() throws Exception {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            logger.info(child);
        }
    }

    /**
     * 删除节点
     *
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception {
        // 参数2：指定要删除的版本，-1表示删除所有版本
        zkClient.delete("/abc", -1);
    }


    /**
     * 获取节点的数据
     *
     * @throws Exception
     */
    @Test
    public void testGetDate() throws Exception {
        byte[] data = zkClient.getData("/eclipse", false, null);
        logger.info(new String(data));
    }

    /**
     * 获取节点的数据
     *
     * @throws Exception
     */
    @Test
    public void testWatcher() throws Exception {
        byte[] data = zkClient.getData("/aa", watchedEvent -> logger.info("{}", watchedEvent), new Stat());
        logger.info(new String(data));
        while (true) {
            Thread.sleep(1000);
        }
    }


}
