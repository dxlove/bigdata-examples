package com.leone.bigdata.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-12
 **/
public class CuratorZkClient {

    private final static Logger logger = LoggerFactory.getLogger(ZkClient.class);

    private static final String CONNECT_ADDR = "ip:2181";

    private static final int SESSION_TIMEOUT = 5000;

    private static CuratorFramework client;

    static {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        // 服务器列表 重试策略 会话超时时间，单位毫秒，默认60000ms 连接创建超时时间，单位毫秒，默认60000ms
        //client = CuratorFrameworkFactory.newClient(
        //        CONNECT_ADDR,
        //        SESSION_TIMEOUT,
        //        3000,
        //        retryPolicy);

        // fluent api 创建 client
        client = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .connectionTimeoutMs(5000)
                .retryPolicy(retryPolicy)
                .build();
    }

    public static void main(String[] args) throws Exception {
        client.start();
        Thread.sleep(8000);
        String path = "/hello";
        String childrenPath = "/hello/world";

        create(client, path, "hello");

        Thread.sleep(3000);

        checkPath(client, path);

        Thread.sleep(3000);

        getData(client, path);

        Thread.sleep(3000);

        create(client, childrenPath, "world");

        Thread.sleep(3000);

        getData(client, childrenPath);

        Thread.sleep(3000);

        delete(client, path);

    }

    /**
     * 创建一个节点，指定创建模式，附带初始化内容：
     *
     * @param path
     * @param content
     * @throws Exception
     */
    private static String create(CuratorFramework client, String path, String content) throws Exception {
        String s = client.create().withMode(CreateMode.PERSISTENT).forPath(path, content.getBytes());
        logger.info("create: {} ", s);
        return s;
    }


    /**
     * 强制保证删除，并且递归删除其所有的子节点
     *
     * @param path
     * @throws Exception
     */
    private static Void delete(CuratorFramework client, String path) throws Exception {
        Void v = client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
        logger.info("delete: {}", v);
        return v;
    }


    /**
     * 读取一个节点的数据内容，同时获取到该节点的 stat
     *
     * @param path
     * @return
     * @throws Exception
     */
    private static Stat getData(CuratorFramework client, String path) throws Exception {
        Stat stat = new Stat();
        byte[] bytes = client.getData().storingStatIn(stat).forPath(path);
        logger.info("getData: {}", new String(bytes));
        return stat;
    }

    /**
     * 更新一个节点的数据内容
     *
     * @param path
     * @return
     * @throws Exception
     */
    private static Stat setData(CuratorFramework client, String path, String content) throws Exception {
        Stat stat = client.setData().forPath(path, content.getBytes());
        logger.info("setData: {}", stat);
        return stat;
    }


    /**
     * 该方法返回一个Stat实例
     *
     * @param path
     * @return
     * @throws Exception
     */
    private static Stat checkPath(CuratorFramework client, String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        logger.info("checkPath: {} ", stat);
        return stat;
    }

    private static List<String> getChildren(CuratorFramework client, String path) throws Exception {
        List<String> children = client.getChildren().forPath(path);
        for (String child : children) {
            logger.info("getChildren: {}", child);
        }
        return children;
    }

}
