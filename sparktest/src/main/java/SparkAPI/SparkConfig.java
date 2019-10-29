package SparkAPI;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class SparkConfig {

    public static void main(String[] args){


        SparkConf sparkConf = new SparkConf();

        //setMaster("local")本地运行模式，不需要安装spark集群
        //sparkConf.setAppName("sparkBoot").setMaster("local");

        //集群运行 standalone模式的client部署方式
        //client部署：client mode下Driver进程运行在client端（提交作业的节点）sparksubmit进程中
        sparkConf.set("master","spark://master:7077");
        sparkConf.set("deploy-mode","client");
        sparkConf.set("class","SparkAPI.SparkTest");
        sparkConf.set("name","TestSparkAPP");

        //集群运行 standalone模式的cluster部署方式
        //cluster部署：Driver程序在worker集群中某个节点。client提交过job后，sparksubmit进程消失
        sparkConf.set("master","spark://master:7077");
        sparkConf.set("deploy-mode","cluster");
        sparkConf.set("class","SparkAPI.SparkTest");
        sparkConf.set("name","TestSparkAPP");

        //集群运行 yarn模式 将上面的master修改成yarn就可以运行了
        //yarn-client  driver运行在client端sparksubmit进程
        //yarn-cluster driver运行在application master上


        JavaSparkContext sc = new JavaSparkContext(sparkConf);



    }


}
