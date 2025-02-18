package com.leone.bigdata.redis.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>使用redis的SETNX实现分布式锁，多个进程执行以下Redis命令：setnx lockKey lockValue
 * SETNX是将 key 的值设为 value，当且仅当 key 不存在。若给定的 key 已经存在，则 SETNX 不做任何动作。
 * <p>
 * 1.返回1，说明该进程获得锁，SETNX将键 lock.id 的值设置为锁的超时时间，当前时间 +加上锁的有效时间。
 * 2.返回0，说明其他进程已经获得了锁，进程不能进入临界区。进程可以在一个循环中不断地尝试 SETNX 操作，以获得锁。
 *
 * @author leone
 * @since 2019-07-19
 **/
public class RedisLock {

    private final static Logger logger = LoggerFactory.getLogger(RedisLock.class);

    private static JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "localhost", 6379, 3000);

    private static Jedis jedis = jedisPool.getResource();

    // 锁的key
    private static String lockKey = "redisLock";

    // 锁过期时间
    private static int expire = 30000;

    private static int index;

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (int i = 0; i < 10; i++) {
            executorService.execute(() -> {
                RedisLock lock = new RedisLock();
                //ExecutorService executor = Executors.newCachedThreadPool();
                //for (int j = 0; j < 10; j++) {
                //    executor.execute(() -> {
                //        if (lock.tryLock()) {
                //            try {
                //                executor();
                //            } finally {
                //
                //                lock.unlock();
                //            }
                //        }
                //    });
                //}

                if (lock.tryLock()) {
                    try {
                        executor();
                    } finally {

                        lock.unlock();
                    }
                }
            });
        }
        executorService.shutdown();
        System.out.println(index);
    }

    /**
     *
     */
    public static void executor() {
        logger.info("begin executor");
        index++;
        logger.info("end executor");
    }

    /**
     * 加锁
     *
     * @return
     */
    public boolean tryLock() {
        long now = System.currentTimeMillis();
        boolean flag = jedis.setnx(lockKey, String.valueOf(now + expire)) == 1;

        if (!flag) {
            String timestamp = jedis.get(lockKey);
            if (Long.parseLong(timestamp) < now) {
                String oldTimestamp = jedis.getSet(lockKey, String.valueOf(now + expire));
                if (oldTimestamp.equals(timestamp)) {
                    jedis.expire(lockKey, expire);
                    return true;
                }
            }
        }
        if (flag) {
            jedis.expire(lockKey, expire);
        }
        return flag;
    }

    /**
     * 解锁
     *
     * @return
     */
    public boolean unlock() {
        return jedis.del(lockKey) == 1;
    }

}
