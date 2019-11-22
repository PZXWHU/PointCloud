package com.pzx.distributedLock;

import org.apache.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.config.Config;
import sun.security.jca.GetInstance;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;


public class RedissonManager implements Serializable {

    private static class RedissonHolder implements Serializable {
        private static Redisson redisson = getRedisson();
        private static Logger logger = Logger.getLogger(RedissonHolder.class);

        private static Redisson getRedisson(){
            try {
                Config config = new Config();
                InputStream redisPropertiesInputStream = RedissonHolder.class.getClassLoader().getResourceAsStream("redis.conf");
                Properties redisProperties = new Properties();
                redisProperties.load(redisPropertiesInputStream);

                String redisIP = redisProperties.getProperty("ip");
                String redisPort = redisProperties.getProperty("port");

                config.useSingleServer().setAddress("redis://"+redisIP+":"+redisPort);
                //得到redisson对象
                redisson = (Redisson) Redisson.create(config);
                return redisson;
            }catch (Exception e){
                e.printStackTrace();
                logger.warn("redission初始化失败");
                return null;
            }
        }

    }

    private RedissonManager (){}

    public static Redisson getInstance(){
        return RedissonHolder.redisson;
    }

}
