package com.pzx.dataSplit;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.pzx.IOUtils;
import com.pzx.geometry.Cube;
import com.pzx.geometry.Cuboid;
import com.pzx.geometry.Grid3D;
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
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.PartitionPruningRDD;
import org.apache.spark.rdd.PartitionPruningRDD$;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.sources.In;
import org.apache.spark.storage.StorageLevel;
import scala.Function1;
import scala.Function2;
import scala.Tuple2;

import scala.reflect.ClassManifestFactory;
import static com.pzx.pointCloud.PointCloud.*;

import java.io.File;
import java.io.Serializable;
import java.util.*;

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

        originDataset.persist(StorageLevel.MEMORY_AND_DISK_SER());

        Dataset<Point3D> point3DDataset = originDataset.map((MapFunction<Row, Point3D>) row->{
            double x = row.getAs("x");
            double y = row.getAs("y");
            double z = row.getAs("z");
            int r = row.getAs("r");
            int g = row.getAs("g");
            int b = row.getAs("b");
            return new Point3D(x,y,z,(byte)r,(byte)g,(byte)b);
        },Encoders.kryo(Point3D.class));



        //创建cloud.js文件
        PointCloud pointCloud = TxtSplit1.createCloudJS(originDataset,outputDirPath);
        logger.info("-----------------------------------生成点云信息文件cloud.js");

        //切分点云
        splitPointCloud(point3DDataset,pointCloud,outputDirPath);
        logger.info("-----------------------------------点云分片任务完成，bin文件全部生成");

        //创建索引文件
        TxtSplit1.createHrcFile(outputDirPath);
        logger.info("-----------------------------------生成索引文件r.hrc");
        logger.info("-----------------------------------此次点云分片任务全部耗时为："+(System.currentTimeMillis()-time));

        sparkSession.stop();
        sparkSession.close();
    }


    /**
     * 有类型的DataSet聚合
     * @param point3DDataset
     * @param outputDirPath
     * @return
     */
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

        double[] coordinatesScale = pointCloud.getScales();

        Tuple2<JavaRDD<Point3D>, OcTreePartitioner> partitionedResultTuple = spatialPartitioning(point3DDataset.toJavaRDD(),pointCloud);
        JavaRDD<Point3D> partitionedRDD = partitionedResultTuple._1;
        OcTreePartitioner ocTreePartitioner = partitionedResultTuple._2;


        JavaRDD<Tuple2<Integer,Point3D>> prunedRDDWithOriginalPartitionID = partitionsPruning(partitionedRDD);
        List<Cuboid> partitionRegions = ocTreePartitioner.getPartitionRegions();
        Cuboid partitionsTotalRegion = ocTreePartitioner.getPartitionsTotalRegions();
        //初始网格一个坐标轴的单元数,网格单元边长
        int initGridOneSideCellNum = 1 << 8;
        double initGridCellSideLength = partitionsTotalRegion.getXSideLength() / initGridOneSideCellNum;
        double[] partitionBoundingBox = partitionsTotalRegion.getBoundingBox();

        //广播变量
        Broadcast<List<Cuboid>> partitionRegionsBroadcast = sparkSession.sparkContext().broadcast(partitionRegions, ClassManifestFactory.classType(List.class));

        prunedRDDWithOriginalPartitionID.mapPartitions((Iterator<Tuple2<Integer,Point3D>> iterator) ->{

            Tuple2<Integer, Point3D> pointWithOriginalPartitionID = iterator.next();
            Cuboid partitionRegion = partitionRegionsBroadcast.getValue().get(pointWithOriginalPartitionID._1);
            Grid3D grid3D = new Grid3D(partitionRegion, initGridCellSideLength);

            grid3D.inset(pointWithOriginalPartitionID._2);
            while (iterator.hasNext()){
                grid3D.inset(iterator.next()._2);
            }

            grid3D.shardToFile(partitionBoundingBox,coordinatesScale,outputDirPath);

            return Collections.emptyIterator();
        }, true).foreach(o -> {});




    }

    /**
     * 利用八叉树对RDD进行分区
     * @param point3DJavaRDD
     * @param pointCloud
     * @return
     */
    public static Tuple2<JavaRDD<Point3D>, OcTreePartitioner> spatialPartitioning(JavaRDD<Point3D> point3DJavaRDD, PointCloud pointCloud){

        double sampleFraction = 0.01;//百分之一
        List<Point3D> samples = point3DJavaRDD.sample(false,sampleFraction).collect();

        int partitionNum = point3DJavaRDD.partitions().size();
        double[] boundingBox = pointCloud.getBoundingBox();
        //将分区范围扩大一点点，避免因浮点数精度问题，导致与边界重合的点不在范围内
        //传入正方体范围以便后面的网格处理
        Cube partitionsTotalRegion = (Cube) new Cube(boundingBox[minX],boundingBox[minY],boundingBox[minZ],(boundingBox[maxX] - boundingBox[minX])).expandLittle();
        OcTreePartitioning ocTreePartitioning = new OcTreePartitioning(samples, partitionsTotalRegion,partitionNum);
        OcTreePartitioner ocTreePartitioner = ocTreePartitioning.getPartitioner();

        //广播变量
        Broadcast<OcTreePartitioner> ocTreePartitionerBroadcast = sparkSession.sparkContext().broadcast(ocTreePartitioner, ClassManifestFactory.classType(OcTreePartitioner.class));

        JavaRDD<Point3D> partitionedRDD = point3DJavaRDD.mapPartitionsToPair(pointIterator -> {

            OcTreePartitioner executorOcTreePartitioner = ocTreePartitionerBroadcast.getValue();
            List<Tuple2<Integer, Point3D>> result = new ArrayList<>();
            while (pointIterator.hasNext()){
                Point3D point3D = pointIterator.next();
                int partitionID = executorOcTreePartitioner.findPartitionIDForObject(point3D);
                result.add(new Tuple2<Integer, Point3D>(partitionID, point3D));
            }
            return result.iterator();
        }).partitionBy(ocTreePartitioner).map( (Tuple2<Integer, Point3D> tuple ) -> tuple._2 );

        return new Tuple2<JavaRDD<Point3D>, OcTreePartitioner>(partitionedRDD, ocTreePartitioner);
    }

    /**
     * 分区裁剪，排除没有数据的分区
     * @param partitionedRDD
     * @return
     */
    public static JavaRDD<Tuple2<Integer,Point3D>> partitionsPruning(JavaRDD<Point3D> partitionedRDD){
        java.util.List<Tuple2<Integer, Boolean>> partitionIsWithElementsList = partitionedRDD.mapPartitionsWithIndex((index, iterator) ->{
            //如果分区为空，则返回false
            if (!iterator.hasNext()){
                return Arrays.asList(new Tuple2<Integer, Boolean>(index, false)).iterator();
            }
            return Arrays.asList(new Tuple2<Integer, Boolean>(index, true)).iterator();
        },true).collect();

        Map<Integer, Boolean> partitionIsWithElementsMap = new HashMap<>();
        for(Tuple2<Integer, Boolean> tuple : partitionIsWithElementsList){
            partitionIsWithElementsMap.put(tuple._1,tuple._2);
        }

        class PartitionPruningFunction implements Function1<Object, Object> , Serializable{
            Map<Integer, Boolean> partitionIsWithElementsMap;
            public PartitionPruningFunction(Map<Integer, Boolean> partitionIsWithElementsMap){
                this.partitionIsWithElementsMap =partitionIsWithElementsMap;
            }
            @Override
            public Boolean apply(Object v1) {
                return partitionIsWithElementsMap.get((int)v1);
            }
        }

        JavaRDD<Tuple2<Integer, Point3D>> partitionedRDDWithPartitionID = partitionedRDD.mapPartitionsWithIndex((index, iterator)->{
            List<Tuple2<Integer, Point3D>> tuple2List = new ArrayList<>();
            while (iterator.hasNext()){
                tuple2List.add(new Tuple2<Integer, Point3D>(index, iterator.next()));
            }
            return tuple2List.iterator();
        }, true);

        //分区裁剪RDD，传入函数，根据分区id计算出布尔值，true则保留分区，false裁剪分区
        PartitionPruningRDD<Tuple2<Integer,Point3D>> prunedRDDWithOriginalPartitionID = PartitionPruningRDD.create(partitionedRDDWithPartitionID.rdd(),
                new PartitionPruningFunction(partitionIsWithElementsMap));

        return prunedRDDWithOriginalPartitionID.toJavaRDD();
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
