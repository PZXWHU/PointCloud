package com.pzx.dataSplit;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.pzx.IOUtils;
import com.pzx.geometry.Cuboid;
import com.pzx.geometry.Point3D;
import com.pzx.pointCloud.PointAttribute;
import com.pzx.pointCloud.PointCloud;
import com.pzx.spatialPartition.OcTreePartitioner;
import com.pzx.spatialPartition.OcTreePartitioning;
import com.pzx.utils.SparkUtils;
import org.apache.commons.io.Charsets;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.PartitionPruningRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 根据正方体网格和八叉树进行点云LOD构建和数据分片
 * 数据八叉树分区
 */
public class TxtSplit2 {

    private static Logger logger = Logger.getLogger(TxtSplit2.class);

    private static SparkSession sparkSession;

    public static void main(String[] args) {

        Preconditions.checkArgument(args.length==2,"inputDirPath and outputDirPath is needed！");

        //输入本地输入路径
        String inputDirPath = args[0];
        //生成结果输出路径
        String outputDirPath = args[1];
        //初始化SparkSession
        sparkSession = SparkUtils.localSparkSessionInit();

        long time = System.currentTimeMillis();

        //读取数据
        Dataset<Row> originDataset = sparkSession.read()
                .format("csv")
                .option("sep"," ")
                .option("inferSchema","true")
                .load(inputDirPath)
                .toDF("x","y","z","intensity","r","g","b")
                .selectExpr("x","y","z","r","g","b");

        Dataset<Point3D> point3DDataset = originDataset.map((MapFunction<Row, Point3D>) row->{
            double x = row.getAs("x");
            double y = row.getAs("y");
            double z = row.getAs("z");
            int r = row.getAs("r");
            int g = row.getAs("g");
            int b = row.getAs("b");
            return new Point3D(x,y,z,(byte)r,(byte)g,(byte)b);
        },Encoders.kryo(Point3D.class));

        point3DDataset.persist(StorageLevel.MEMORY_AND_DISK_SER());

        //创建cloud.js文件
        PointCloud pointCloud = createCloudJS(point3DDataset,outputDirPath);
        logger.info("-----------------------------------生成点云信息文件cloud.js");

        //切分点云
        splitPointCloud(point3DDataset,pointCloud,outputDirPath);
        logger.info("-----------------------------------点云分片任务完成，bin文件全部生成");

    }


    public static PointCloud createCloudJS(Dataset<Point3D> point3DDataset, String  outputDirPath){

        class CreatePointCloudUDAF extends Aggregator<Point3D, PointCloud, PointCloud> {

            @Override
            public PointCloud zero() {
                PointCloud pointCloud = new PointCloud();
                pointCloud.setPoints(0);
                pointCloud.setTightBoundingBox(new double[]{-Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE,
                        Double.MAX_VALUE,Double.MAX_VALUE,Double.MAX_VALUE});
                return pointCloud;
            }

            @Override
            public PointCloud reduce(PointCloud pointCloud, Point3D point3D) {
                pointCloud.setPoints(pointCloud.getPoints() + 1);
                double[] tightBoundingBox = pointCloud.getTightBoundingBox();

                tightBoundingBox[0] = Math.max(point3D.x, tightBoundingBox[0]);
                tightBoundingBox[1] = Math.max(point3D.y, tightBoundingBox[1]);
                tightBoundingBox[2] = Math.max(point3D.z, tightBoundingBox[2]);

                tightBoundingBox[3] = Math.min(point3D.x,tightBoundingBox[3]);
                tightBoundingBox[4] = Math.min(point3D.y,tightBoundingBox[4]);
                tightBoundingBox[5] = Math.min(point3D.z,tightBoundingBox[5]);

                return pointCloud;
            }

            @Override
            public PointCloud merge(PointCloud pointCloud1, PointCloud pointCloud2) {

                pointCloud1.setPoints(pointCloud1.getPoints() + pointCloud2.getPoints());
                double[] tightBoundingBox1 = pointCloud1.getTightBoundingBox();
                double[] tightBoundingBox2 = pointCloud2.getTightBoundingBox();

                tightBoundingBox1[0] = Math.max(tightBoundingBox1[0], tightBoundingBox2[0]);
                tightBoundingBox1[1] = Math.max(tightBoundingBox1[1], tightBoundingBox2[1]);
                tightBoundingBox1[2] = Math.max(tightBoundingBox1[2], tightBoundingBox2[2]);

                tightBoundingBox1[3] = Math.min(tightBoundingBox1[3], tightBoundingBox2[3]);
                tightBoundingBox1[4] = Math.min(tightBoundingBox1[4], tightBoundingBox2[4]);
                tightBoundingBox1[5] = Math.min(tightBoundingBox1[5], tightBoundingBox2[5]);

                return pointCloud1;
            }

            @Override
            public PointCloud finish(PointCloud pointCloud) {
                pointCloud.setBoundingBox(pointCloud.createBoundingBox(pointCloud.getTightBoundingBox()));
                pointCloud.setScales(new double[]{0.001,0.001,0.001});
                pointCloud.setPointAttributes(Arrays.asList(PointAttribute.POSITION_XYZ, PointAttribute.RGB));
                return pointCloud;
            }

            @Override
            public Encoder<PointCloud> bufferEncoder() { return Encoders.kryo(PointCloud.class); }

            @Override
            public Encoder<PointCloud> outputEncoder() { return Encoders.kryo(PointCloud.class); }
        }

        //有类型限制的聚合
        CreatePointCloudUDAF createPointCloudUDAF = new CreatePointCloudUDAF();
        TypedColumn<Point3D, PointCloud> aggPointCloud = createPointCloudUDAF.toColumn();
        Dataset<PointCloud> pointCloudDataSet  = point3DDataset.select(aggPointCloud);
        PointCloud pointCloud = pointCloudDataSet.collectAsList().get(0);

        JSONObject cloudJS = pointCloud.buildCloudJS();
        IOUtils.writerDataToFile(outputDirPath+ File.separator+"cloud.js",cloudJS.toJSONString().getBytes(Charsets.UTF_8),false);
        return pointCloud;

    }



    public static void splitPointCloud(Dataset<Point3D> point3DDataset, PointCloud pointCloud,String outputDirPath){

        //广播变量
        double[] boundingBox = pointCloud.getBoundingBox();
        double[] scale = pointCloud.getScales();

        //如果tightBoundingBox某一边小于其他边10倍的话，采用四叉树分片
        double[] tightBoundingBox = pointCloud.getTightBoundingBox();



        JavaRDD<Point3D> partitionedRDD = spatialPartitioning(point3DDataset.toJavaRDD(),pointCloud);
        System.out.println(partitionedRDD.partitions().size());
        JavaRDD<Point3D> prunedRDD = partitionsPruning(partitionedRDD);


    }

    /**
     * 利用八叉树对RDD进行分区
     * @param point3DJavaRDD
     * @param pointCloud
     * @return
     */
    public static JavaRDD<Point3D> spatialPartitioning(JavaRDD<Point3D> point3DJavaRDD, PointCloud pointCloud){

        double sampleFraction = 0.01;//百分之一
        List<Point3D> samples = point3DJavaRDD.sample(false,sampleFraction).collect();

        double[] boundingBox = pointCloud.getBoundingBox();
        //将分区范围扩大一点点，避免因浮点数精度问题，导致与边界重合的点不在范围内
        Cuboid partitionTotalRegion = new Cuboid(boundingBox[3],boundingBox[4],boundingBox[5],boundingBox[0],boundingBox[1],boundingBox[2]).expandLittle();
        int partitionNum = point3DJavaRDD.partitions().size();

        OcTreePartitioning ocTreePartitioning = new OcTreePartitioning(samples, partitionTotalRegion,partitionNum);
        OcTreePartitioner ocTreePartitioner = ocTreePartitioning.getPartitioner();

        JavaRDD<Point3D> partitionedRDD = point3DJavaRDD.mapToPair(point3D -> {
            int partitionID = ocTreePartitioner.findPartitionIDForObject(point3D);
            return new Tuple2<Integer, Point3D>(partitionID, point3D);
        }).partitionBy(ocTreePartitioner).map( (Tuple2<Integer, Point3D> tuple ) -> tuple._2 );

        return partitionedRDD;

    }

    /**
     * 分区裁剪，排除没有数据的分区
     * @param partitionedRDD
     * @return
     */
    public static JavaRDD<Point3D> partitionsPruning(JavaRDD<Point3D> partitionedRDD){
        List<Tuple2<Integer, Boolean>> partitionIsWithElements = partitionedRDD.mapPartitionsWithIndex((index, iterator) ->{
            //如果分区为空，则返回null
            if (!iterator.hasNext()){
                return Arrays.asList(new Tuple2<Integer, Boolean>(index, false)).iterator();
            }
            return Arrays.asList(new Tuple2<Integer, Boolean>(index, true)).iterator();
        },true).collect();

        Map<Integer, Boolean> partitionPruningMap = new HashMap<>();
        for(Tuple2<Integer, Boolean> tuple : partitionIsWithElements){
            partitionPruningMap.put(tuple._1,tuple._2);
        }

        PartitionPruningRDD<Point3D> partitionPruningRDD = new PartitionPruningRDD<Point3D>(partitionedRDD.rdd(),
                (partitioneIndex)->partitionPruningMap.get(partitioneIndex), ClassManifestFactory.classType(Point3D.class));

        return partitionPruningRDD.toJavaRDD();
    }

    /** 可用于建立全局索引
     List<Tuple3<Integer, Cuboid,Integer>> result = partitionedDataset.toJavaRDD().mapPartitionsWithIndex((index, iterator) ->{
     //如果分区为空，则返回null
     if (!iterator.hasNext()){
     return Arrays.asList(new Tuple3<Integer, Cuboid,Integer>(index, null,0)).iterator();
     }

     double minX = Double.MAX_VALUE;
     double minY = Double.MAX_VALUE;
     double minZ = Double.MAX_VALUE;
     double maxX = -Double.MAX_VALUE;
     double maxY = -Double.MAX_VALUE;
     double maxZ = -Double.MAX_VALUE;
     int pointNum = 0;
     do{
     pointNum ++;
     Point3D point3D = iterator.next();
     minX = Math.min(minX,point3D.x);
     minY = Math.min(minY,point3D.y);
     minZ = Math.min(minZ,point3D.z);
     maxX = Math.max(maxX,point3D.x);
     maxY = Math.max(maxY,point3D.y);
     maxZ = Math.max(maxZ,point3D.z);
     }while (iterator.hasNext());

     Cuboid partitionRegion = new Cuboid(minX,minY,minZ,maxX,maxY,maxZ);
     return Arrays.asList(new Tuple3<>(index, partitionRegion,pointNum)).iterator();

     },true).collect();

     System.out.println(result);
     */



}
