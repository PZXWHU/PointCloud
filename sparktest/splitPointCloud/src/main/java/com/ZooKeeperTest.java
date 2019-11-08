package com;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.omg.Messaging.SyncScopeHelper;

import java.io.IOException;

public class ZooKeeperTest {

    public static void main(String[] args)throws Exception {
        ZooKeeper zkClient = new ZooKeeper("47.112.97.110:2181", 120000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getState());
            }
        });
        zkClient.create("/pzx", "".getBytes(), null, CreateMode.EPHEMERAL);



    }
}
