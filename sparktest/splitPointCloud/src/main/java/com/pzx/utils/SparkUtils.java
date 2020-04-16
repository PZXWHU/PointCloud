package com.pzx.utils;

import com.google.common.base.Preconditions;
import com.pzx.split.LasSplit;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SparkUtils {

    private static Logger logger = Logger.getLogger(SparkUtils.class);


    /**
     * 从内部定义的配置文件初始化并设置SparkConf
     * @param confFileName 内部自定义的spark配置文件
     * @return
     */
    private static SparkConf loadSparkConf(String confFileName){

        //sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        //sparkConf.registerKryoClasses(new Class[]{String[].class,String.class,List.class});
        //sparkConf.set("spark.executor.extraJavaOptions","-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/tmp/gc.log");//输出GC日志
        //sparkConf.set("spark.driver.extraJavaOptions","-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/tmp/gc.log");
        //sparkConf.set("spark.executor.memory","4g");

        SparkConf sparkConf = new SparkConf();
        try {
            InputStream sparkPropertiesInputStream = LasSplit.class.getClassLoader().getResourceAsStream(confFileName);
            Properties sparkProperties = new Properties();
            sparkProperties.load(sparkPropertiesInputStream);

            for(String sparkPropertyNames:sparkProperties.stringPropertyNames()){
                sparkConf.set(sparkPropertyNames,sparkProperties.getProperty(sparkPropertyNames));
            }
        }catch (IOException e){
            logger.warn("加载spark配置文件失败");
            sparkConf = null;
        }
        return sparkConf;
    }


    /**
     * 初始化JavaSparkContext
     * @return
     */
    public static JavaSparkContext scInit(){

        SparkConf sparkConf = loadSparkConf("spark.conf");
        Preconditions.checkNotNull(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        return sc;

    }

    /**
     * 初始化SparkSession
     * @return
     */
    public static SparkSession ssInit(){

        SparkConf sparkConf = loadSparkConf("spark.conf");
        Preconditions.checkNotNull(sparkConf);
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        return spark;

    }


}
