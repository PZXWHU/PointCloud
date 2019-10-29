import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Test {

    public static void main(String[] args) throws IOException {

        //setMaster("local")本地运行模式，不需要安装spark集群
        SparkConf sparkConf = new SparkConf();


        sparkConf.setAppName("sparkBoot").setMaster("yarn");


        JavaSparkContext sc = new JavaSparkContext(sparkConf);


        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd = sc.parallelize(data);
        rdd.map((Integer x)->x*2).collect().forEach(x->System.out.println(x));
    }


}
