package SparkAPI;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.List;

public class SparkTest {
    public static void main(String[] args){

        //setMaster("local")本地运行模式，不需要安装spark集群
        SparkConf sparkConf = new SparkConf();


        sparkConf.setAppName("sparkBoot").setMaster("yarn");


        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd = sc.parallelize(data);
        rdd.map((Integer x)->x*2).collect().forEach(x->System.out.println(x));


        rdd.checkpoint();
        /*
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();


         */

        //JavaStreamingContext streamingContext = null;
        //streamingContext.start();


        //RDD之间的转换只是对一个对象的反复包装(包装iterator方法)，记录下了对分区的操作！！！！并没有进行实际的操作
        //正在计算的时候是执行iterator方法

    }



}
