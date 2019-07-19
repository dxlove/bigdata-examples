package com.leone.bigdata.zookeeper.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-30
 **/
public class ZkLock {

    private final static Logger logger = LoggerFactory.getLogger(ZkLock.class);

    private static final String LOCK_ROOT_PATH = "/lock";

    private static final String LOCK_NODE_NAME = "/lock_" + System.currentTimeMillis();

    private static final String CONNECTION_STRING = "ip:2181";

    private static CountDownLatch countDownLatch = new CountDownLatch(1);

    private static Random random = new Random();

    private static ZooKeeper zooKeeper;

    private static int index;

    private String lockPath;

    // 监控lockPath的前一个节点的watcher
    private final Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            logger.info(event.toString());
            synchronized (this) {
                notifyAll();
            }
        }
    };

    static {
        try {
            zooKeeper = new ZooKeeper(CONNECTION_STRING, 10000, event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    logger.info("connection zookeeper success... {}", event.getState());
                    countDownLatch.countDown();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        countDownLatch.await();
        ExecutorService executorService = Executors.newCachedThreadPool();

        ZkLock lock = new ZkLock();
        for (int i = 0; i < 10; i++) {
            executorService.execute(() -> {
                try {
                    lock.acquireLock();
                    process();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    lock.releaseLock();
                }
            });
        }
        executorService.shutdown();
    }

    /**
     * 业务逻辑
     *
     * @throws InterruptedException
     */
    private static void process() throws InterruptedException {
        System.out.println("---------- process start ----------");
        Thread.sleep(random.nextInt(500));
        index++;
        System.out.println("---------- process end ----------");
    }

    /**
     * 加锁
     *
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void acquireLock() throws InterruptedException, KeeperException {
        // 如果根节点不存在，则创建根节点
        Stat stat = zooKeeper.exists(LOCK_ROOT_PATH, false);
        if (stat == null) {
            zooKeeper.create(LOCK_ROOT_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        // 创建 EPHEMERAL_SEQUENTIAL 类型零时节点 /lock/lock_15635252328350000000072
        lockPath = zooKeeper.create(LOCK_ROOT_PATH + LOCK_NODE_NAME, Thread.currentThread().getName().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        logger.info(Thread.currentThread().getName() + " lock: {}", lockPath);
        // 尝试获取锁
        attemptLock();
    }


    /**
     * 释放锁
     */
    public void releaseLock() {
        try {
            zooKeeper.delete(lockPath, -1);
            logger.info(Thread.currentThread().getName() + " unlock: {}", lockPath);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取锁
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void attemptLock() throws KeeperException, InterruptedException {
        // 获取Lock所有子节点，放弃使用会话的监听器 [lock_15635252328350000000072, lock_15635252328350000000073, lock_15635252328350000000074]
        List<String> lockPaths = zooKeeper.getChildren(LOCK_ROOT_PATH, false);

        // 把获取到的节点排序
        Collections.sort(lockPaths);

        // 获取当前锁在 list 的下标：例如获取 lock_15635252328350000000073 在 list 中的下标
        int index = lockPaths.indexOf(lockPath.substring(LOCK_ROOT_PATH.length() + 1));

        // 如果 lockPath 是序号最小的节点，则获取锁
        if (index == 0) {
            System.out.println(Thread.currentThread().getName() + " getLock lockPath: " + lockPath);
        } else {
            // 获取前一个lock，监控前一个节点
            String preLock = lockPaths.get(index - 1);
            Stat stat = zooKeeper.exists(LOCK_ROOT_PATH + "/" + preLock, watcher);
            // 假如前一个节点不存在了，比如说执行完毕，或者执行节点掉线，重新获取锁
            //if (stat == null) {
            //    attemptLock();
            //    // 阻塞当前进程，直到preLockPath释放锁，被watcher观察到，notifyAll后，重新acquireLock
            //} else {
            //    logger.info("wait preLock unlock path: " + preLock);
            //    synchronized (watcher) {
            //        watcher.wait();
            //    }
            //    attemptLock();
            //}
        }
    }


}
