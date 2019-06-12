package com.leone.bigdata.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * <p>
 *
 * @author leone
 * @since 2019-06-12
 **/
public class CuratorBase {

    //  static final String CONNECT_ADDR = "192.168.1.171:2181,192.168.1.172:2181,192.168.1.173:2181";
    private static final String CONNECT_ADDR = "node-1:2181,node-2,node-3";

    // session超时时间ms
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
                .namespace("base")
                .build();
    }

    public static void main(String[] args) throws Exception {
        client.start();
        //create("/test", "hello world");
        //delete("/test");
        String data = getData("/test");
        System.out.println(data);
    }

    /**
     * 创建一个节点，指定创建模式，附带初始化内容：
     *
     * @param path
     * @param content
     * @throws Exception
     */
    private static void create(String path, String content) throws Exception {
        client.create().withMode(CreateMode.PERSISTENT).forPath(path, content.getBytes());
    }


    /**
     * 强制保证删除，并且递归删除其所有的子节点
     *
     * @param path
     * @throws Exception
     */
    private static void delete(String path) throws Exception {
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
    }


    /**
     * 读取一个节点的数据内容，同时获取到该节点的stat
     *
     * @param path
     * @return
     * @throws Exception
     */
    private static String getData(String path) throws Exception {
        Stat stat = new Stat();
        byte[] bytes = client.getData().storingStatIn(stat).forPath(path);
        System.out.println(stat.toString());
        return new String(bytes);
    }

    /**
     * 更新一个节点的数据内容
     *
     * @param path
     * @return
     * @throws Exception
     */
    private static Stat setData(String path, String content) throws Exception {
        return client.setData().forPath(path, content.getBytes());
    }


    /**
     * 该方法返回一个Stat实例
     *
     * @param path
     * @return
     * @throws Exception
     */
    private static Stat checkPath(String path) throws Exception {
        return client.checkExists().forPath(path);
    }


}
