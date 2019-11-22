package com.pzx;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.InputStream;
import java.util.Properties;

public class SparkUtils {

    private static Logger logger = Logger.getLogger(SparkUtils.class);

    /**
     * 初始化JavaSparkContext
     * @return
     */
    public static JavaSparkContext scInit(){

        try {
            InputStream sparkPropertiesInputStream = LasSplit.class.getClassLoader().getResourceAsStream("spark.conf");
            Properties sparkProperties = new Properties();
            sparkProperties.load(sparkPropertiesInputStream);

            SparkConf sparkConf = new SparkConf();
            for(String sparkPropertyNames:sparkProperties.stringPropertyNames()){
                sparkConf.set(sparkPropertyNames,sparkProperties.getProperty(sparkPropertyNames));
            }

            //sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
            //sparkConf.registerKryoClasses(new Class[]{String[].class,String.class,List.class});
            //sparkConf.set("spark.executor.extraJavaOptions","-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/tmp/gc.log");//输出GC日志
            //sparkConf.set("spark.driver.extraJavaOptions","-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/tmp/gc.log");
            //sparkConf.set("spark.executor.memory","4g");
            JavaSparkContext sc = new JavaSparkContext(sparkConf);
            return sc;

        }catch (Exception e){
            e.printStackTrace();
            logger.warn("sparkContext初始化失败！");
            return null;
        }




    }
}
