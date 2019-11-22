package com.pzx.distributedLock;

import org.apache.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.api.RLock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DistributedRedisLock implements Serializable {

    private static Redisson redisson = RedissonManager.getInstance();
    private static final String LOCK_TITLE = "redisLock_";
    private static Logger logger = Logger.getLogger(DistributedRedisLock.class);


    //加锁
    public static boolean lock(String lockName){
        long time = System.currentTimeMillis();
        //声明key对象
        String key = LOCK_TITLE + lockName;
        //获取锁对象
        RLock mylock = redisson.getLock(key);
        //加锁，并且设置锁过期时间，防止死锁的产生
        mylock.lock(2, TimeUnit.MINUTES);
        System.out.println("------------------------------------------加锁成功："+lockName);
        logger.info("------------------------------------------加锁成功："+lockName);
        //加锁成功
        System.out.println("加锁耗时"+(System.currentTimeMillis()-time));
        return  true;
    }
    //锁的释放
    public static void unlock(String lockName){
        long time = System.currentTimeMillis();
        //必须是和加锁时的同一个key
        String key = LOCK_TITLE + lockName;
        //获取所对象
        RLock mylock = redisson.getLock(key);
        //释放锁（解锁）
        mylock.unlock();
        System.out.println("------------------------------------------解锁成功："+lockName);
        logger.info("------------------------------------------解锁成功："+lockName);
        System.out.println("解锁耗时"+(System.currentTimeMillis()-time));
    }

}
