package com.pzx;

import com.pzx.geometry.Point3D;
import com.pzx.utils.SparkUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.PartitionPruningRDD;
import org.apache.spark.rdd.PruneDependency;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Function1;
import scala.Function2;
import scala.Serializable;
import scala.Tuple2;
import scala.collection.Seq;
import scala.reflect.ClassManifestFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;
import java.util.stream.Collectors;

public class Test {
    public static void main(String[] args)throws Exception {

/*
        SparkSession sparkSession = SparkUtils.localSparkSessionInit();

        String inputDirPath = "file:///D:\\wokspace\\点云的储存与可视化\\大数据集与工具\\data\\test.txt";

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("x", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("y", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("z", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("intensity", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("r", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("g", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("b", DataTypes.IntegerType, false));
        StructType scheme = DataTypes.createStructType(fields);

        //读取数据
        Dataset<Row> dataset = sparkSession.read()
                .format("csv")
                .schema(scheme)
                .option("sep"," ")
                //.option("inferSchema","true")
                .load(inputDirPath);

                //.selectExpr("x","y","z","r","g","b");

       //System.out.println(Math.pow(10,0-1));
        //Dataset<Row> cachedDataSet = dataset.persist(StorageLevel.MEMORY_AND_DISK_SER());
*/
        //创建cloud.js文件
        //PointCloud pointCloud = TxtSplit1.createCloudJS(dataset,"C:\\Users\\PZX\\Desktop\\新建文件夹");
        /*
        Dataset<Point3D> point3DDataset = dataset.map((MapFunction<Row, Point3D>) row->{
            double x = row.getAs("x");
            double y = row.getAs("y");
            double z = row.getAs("z");
            int r = row.getAs("r");
            int g = row.getAs("g");
            int b = row.getAs("b");
            return new Point3D(x,y,z,(byte)r,(byte)g,(byte)b);
        }, Encoders.kryo(Point3D.class));

         */


/*
        dataset.limit(2).show();

        Thread.sleep(100000);

 */





/*

        BufferedReader fileReader = new BufferedReader(new FileReader("D:\\wokspace\\点云的储存与可视化\\大数据集与工具\\data\\sg27_station10_intensity_rgb.txt"));

        FileWriter fileWriter = new FileWriter("D:\\wokspace\\点云的储存与可视化\\大数据集与工具\\data\\test.txt");

        for(int i =0 ;i<1000000;i++){
            fileWriter.write(fileReader.readLine()+"\n");
        }
        fileWriter.close();


 */


        //System.out.println(89.455/0.04367919921875);



    }
}
