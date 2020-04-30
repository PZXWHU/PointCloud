package com.pzx;


import org.apache.hadoop.conf.Configuration;

public class Test {

    public static void main(String[] args)throws Exception {
        Configuration configuration = HBaseUtils.getConfiguration();
        System.out.println(configuration.get("hbase.rootdir"));
        System.out.println(configuration.get("hadoop.tmp.dir"));
        System.out.println(configuration.get("zookeeper.recovery.retry.maxsleeptime"));

        System.out.println(configuration.get("hbase.regionserver.executor.openregion.threads"));
        HBaseUtils.listAllTables();




    }

}
