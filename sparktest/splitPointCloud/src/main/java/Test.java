import com.pzx.LasSplit;
import com.pzx.distributedLock.DistributedRedisLock;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.io.*;
import java.util.*;

public class Test {

    public static Logger logger = Logger.getLogger(Test.class);

    public static void main(String[] args) throws IOException {


        /*
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("spilt pointCloud").setMaster("spark://master:7077");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);




        List list = new ArrayList();
        for(int i=0;i<10000000;i++)
            list.add(new byte[12]);

        long time = System.currentTimeMillis();
        JavaRDD<byte[]> rdd = sc.parallelize(list,20);
        JavaPairRDD<Integer,Integer> pairRDD = rdd.mapToPair(x->{return new Tuple2<>((int)(Math.random()*1000),(int)(Math.random()*1000));});
        pairRDD.combineByKey((x) -> {
                    List list1 = new ArrayList();
                    list1.add(x);
                    return list1;
                },
                (list2, objects) -> {
                    list2.add(objects);
                    return list2;
                },
                (list1,list2) -> {
                    list1.addAll(list2);
                    return list1;
                }, new Partitioner() {
                    @Override
                    public int getPartition(Object key) {
                        return Math.abs(key.toString().hashCode()%numPartitions());
                    }

                    @Override
                    public int numPartitions() {
                        return 24;
                    }
                }).foreach((x)->System.out.println(x));


        System.out.println("--------------耗时为："+(System.currentTimeMillis()-time));

         */

        //Byte[] bytes = new Byte[1024];
        InputStream sparkPropertiesInputStream = Test.class.getClassLoader().getResourceAsStream("spark.conf");
        Properties sparkProperties = new Properties();
        sparkProperties.load(sparkPropertiesInputStream);


        for(String sparkPropertyNames:sparkProperties.stringPropertyNames()){
            System.out.println(sparkPropertyNames);
        }

    }

    public static  void bytesReverse(byte[] bytes){

    }

}
