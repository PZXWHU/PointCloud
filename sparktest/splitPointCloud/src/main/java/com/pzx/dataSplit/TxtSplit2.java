package com.pzx.dataSplit;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.pzx.IOUtils;
import com.pzx.geometry.*;
import com.pzx.pointCloud.HrcFile;
import com.pzx.pointCloud.PointAttribute;
import com.pzx.pointCloud.PointCloud;
import com.pzx.spatialPartition.OcTreePartitioner;
import com.pzx.spatialPartition.OcTreePartitioning;
import com.pzx.utils.SparkUtils;
import jodd.inex.InExRuleMatcher;
import org.apache.commons.io.Charsets;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.PartitionPruningRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Function1;
import scala.Tuple2;

import scala.reflect.ClassManifestFactory;
import scala.runtime.AbstractFunction1;

import static com.pzx.pointCloud.PointCloud.*;

import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 根据正方体网格和八叉树进行点云LOD构建和数据分片
 * 数据八叉树分区
 */
public class TxtSplit2 {


    /**
     * 目前存在两大问题  gc时间过长   java数据结构使用过多 占用内存过大
     * 数据分区不均匀  导致某些Task时间过长
     */

    private static Logger logger = Logger.getLogger(TxtSplit2.class);

    private static SparkSession sparkSession;



    public static void main(String[] args) {

        Preconditions.checkArgument(args.length==2,"inputDirPath and outputDirPath is needed！");

        //输入本地输入路径
        String inputDirPath = args[0];
        //生成结果输出路径
        String outputDirPath = args[1];
        //初始化SparkSession
        sparkSession = SparkUtils.sparkSessionInit();

        long totalTime = System.currentTimeMillis();
        long time = System.currentTimeMillis();
        logger.info("-----------------------------------此次分片任务开始");

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
        Dataset<Row> originDataset = sparkSession.read()
                .format("csv")
                .option("sep"," ")
                .schema(scheme)
                //.option("inferSchema","true") 模式推理会导致源数据被加载两遍
                .load(inputDirPath)
                //.toDF("x","y","z","intensity","r","g","b")
                .selectExpr("x","y","z","r","g","b");

        Dataset<Row> cachedDataSet = originDataset.persist(StorageLevel.MEMORY_AND_DISK_SER());

        Dataset<Point3D> point3DDataset = cachedDataSet.map((MapFunction<Row, Point3D>) row->{
            double x = (double)row.getAs("x");
            double y = (double)row.getAs("y");
            double z = (double)row.getAs("z");
            int r = (int)row.getAs("r");
            int g = (int)row.getAs("g");
            int b = (int)row.getAs("b");
            return new Point3D(x,y,z,(byte)r,(byte)g,(byte)b);
        },Encoders.kryo(Point3D.class));



        //创建cloud.js文件
        PointCloud pointCloud = TxtSplit1.createCloudJS(cachedDataSet,outputDirPath);
        logger.info("-----------------------------------生成点云信息文件cloud.js, 耗时："+(System.currentTimeMillis()-time));

        time = System.currentTimeMillis();
        //切分点云
        List<Tuple2<String, Integer>> nodeElementsNumTupleList = splitPointCloud(point3DDataset,pointCloud,outputDirPath);
        logger.info("-----------------------------------点云分片任务完成，bin文件全部生成, 耗时："+(System.currentTimeMillis()-time));

        time = System.currentTimeMillis();
        //创建索引文件
        HrcFile.createHrcFileWithElementsNum(nodeElementsNumTupleList, outputDirPath);
        logger.info("-----------------------------------生成索引文件r.hrc, 耗时："+(System.currentTimeMillis()-time));

        logger.info("-----------------------------------此次点云分片任务全部耗时为："+(System.currentTimeMillis()-totalTime));

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
                pointCloud.setTightBoundingBox(new Cuboid(Double.MAX_VALUE,Double.MAX_VALUE,Double.MAX_VALUE,
                        -Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE ));
                return pointCloud;
            }
            @Override
            public PointCloud reduce(PointCloud pointCloud, Point3D point3D) {
                pointCloud.setPoints(pointCloud.getPoints() + 1);
                Cuboid tightBoundingBox = pointCloud.getTightBoundingBox();

                tightBoundingBox.setMaxX(Math.max(point3D.x, tightBoundingBox.getMaxX()));
                tightBoundingBox.setMaxY(Math.max(point3D.y, tightBoundingBox.getMaxY()));
                tightBoundingBox.setMaxZ(Math.max(point3D.z, tightBoundingBox.getMaxZ()));
                tightBoundingBox.setMinX(Math.min(point3D.x,tightBoundingBox.getMinX()));
                tightBoundingBox.setMinY(Math.min(point3D.y,tightBoundingBox.getMinY()));
                tightBoundingBox.setMaxZ(Math.min(point3D.z,tightBoundingBox.getMinZ()));

                return pointCloud;
            }
            @Override
            public PointCloud merge(PointCloud pointCloud1, PointCloud pointCloud2) {
                pointCloud1.setPoints(pointCloud1.getPoints() + pointCloud2.getPoints());
                Cuboid tightBoundingBox1 = pointCloud1.getTightBoundingBox();
                Cuboid tightBoundingBox2 = pointCloud2.getTightBoundingBox();

                tightBoundingBox1.setMaxX(Math.max(tightBoundingBox1.getMaxX(), tightBoundingBox2.getMaxX()));
                tightBoundingBox1.setMaxY(Math.max(tightBoundingBox1.getMaxY(), tightBoundingBox2.getMaxY()));
                tightBoundingBox1.setMaxZ(Math.max(tightBoundingBox1.getMaxZ(), tightBoundingBox2.getMaxZ()));
                tightBoundingBox1.setMinX(Math.min(tightBoundingBox1.getMinX(), tightBoundingBox2.getMinX()));
                tightBoundingBox1.setMinY(Math.min(tightBoundingBox1.getMinY(), tightBoundingBox2.getMinY()));
                tightBoundingBox1.setMinZ(Math.min(tightBoundingBox1.getMinZ(), tightBoundingBox2.getMinZ()));

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



    public static List<Tuple2<String, Integer>> splitPointCloud(Dataset<Point3D> point3DDataset, PointCloud pointCloud,String outputDirPath){

        double[] coordinatesScale = pointCloud.getScales();

        Tuple2<JavaPairRDD<Integer, Point3D>, OcTreePartitioner> partitionedResultTuple = spatialPartitioning(point3DDataset.toJavaRDD(),pointCloud);
        JavaPairRDD<Integer, Point3D> partitionedRDDWithPartitionID = partitionedResultTuple._1;
        OcTreePartitioner ocTreePartitioner = partitionedResultTuple._2;

        JavaRDD<Tuple2<Integer,Point3D>> prunedRDDWithOriginalPartitionID = partitionsPruning(partitionedRDDWithPartitionID);
        logger.info("----------------------------------重分区之后的RDD的分区数："+ partitionedRDDWithPartitionID.partitions().size());
        logger.info("----------------------------------剪枝优化之后的RDD的分区数："+ prunedRDDWithOriginalPartitionID.partitions().size());

        List<Cuboid> partitionRegions = ocTreePartitioner.getPartitionRegions();
        Cube partitionsTotalRegion = ocTreePartitioner.getPartitionsTotalRegions().toBoundingBoxCube();

        //初始网格一个坐标轴的单元数,网格单元边长
        int initGridOneSideCellNum = 1 << 5;
        double initGridCellSideLength = partitionsTotalRegion.getXSideLength() / initGridOneSideCellNum;
        //double[] partitionsBoundingBox = partitionsTotalRegion.getBoundingBox();

        //广播变量
        Broadcast<List<Cuboid>> partitionRegionsBroadcast = sparkSession.sparkContext().broadcast(partitionRegions, ClassManifestFactory.classType(List.class));

        List<Tuple2<String, Integer>> nodeElementsNumTupleList = prunedRDDWithOriginalPartitionID.mapPartitions((Iterator<Tuple2<Integer,Point3D>> iterator) ->{

            Tuple2<Integer, Point3D> pointWithOriginalPartitionID = iterator.next();
            Cuboid partitionRegion = partitionRegionsBroadcast.getValue().get(pointWithOriginalPartitionID._1);
            Grid3D grid3D = new Grid3D(partitionRegion, initGridCellSideLength, partitionsTotalRegion);

            grid3D.insert(pointWithOriginalPartitionID._2);
            while (iterator.hasNext()){
                grid3D.insert(iterator.next()._2);
            }

            List<Tuple2<String, Integer>> partitionNodeElementsNumTupleList = grid3D.shardToFile(coordinatesScale,outputDirPath);

            return partitionNodeElementsNumTupleList.iterator();
        }, true).collect();

        return nodeElementsNumTupleList;
    }

    /**
     * 利用八叉树对RDD进行分区
     * @param point3DJavaRDD
     * @param pointCloud
     * @return
     */
    public static Tuple2<JavaPairRDD<Integer,Point3D>, OcTreePartitioner> spatialPartitioning(JavaRDD<Point3D> point3DJavaRDD, PointCloud pointCloud){

        double sampleFraction = 0.01;//百分之一
        List<Point3D> samples = point3DJavaRDD.sample(false,sampleFraction).collect();

        int partitionNum = point3DJavaRDD.partitions().size();
        Cube boundingBox = pointCloud.getBoundingBox();
        //将分区范围扩大一点点，避免因浮点数精度问题，导致与边界重合的点不在范围内
        //传入正方体范围以便后面的网格处理
        Cube partitionsTotalRegion = (Cube) boundingBox.expandLittle();

        OcTreePartitioning ocTreePartitioning = new OcTreePartitioning(samples, partitionsTotalRegion,partitionNum);
        OcTreePartitioner ocTreePartitioner = ocTreePartitioning.getPartitioner();

        //广播变量
        Broadcast<OcTreePartitioner> ocTreePartitionerBroadcast = sparkSession.sparkContext().broadcast(ocTreePartitioner, ClassManifestFactory.classType(OcTreePartitioner.class));

        JavaPairRDD<Integer,Point3D> partitionedRDDWithPartitionID = point3DJavaRDD.mapPartitionsToPair(pointIterator -> {

            OcTreePartitioner executorOcTreePartitioner = ocTreePartitionerBroadcast.getValue();
            List<Tuple2<Integer, Point3D>> result = new ArrayList<>();
            while (pointIterator.hasNext()){
                Point3D point3D = pointIterator.next();
                int partitionID = executorOcTreePartitioner.findPartitionIDForObject(point3D);
                result.add(new Tuple2<Integer, Point3D>(partitionID, point3D));
            }
            return result.iterator();
        }).partitionBy(ocTreePartitioner);

        return new Tuple2<JavaPairRDD<Integer,Point3D>, OcTreePartitioner>(partitionedRDDWithPartitionID, ocTreePartitioner);
    }

    /**
     * 分区裁剪，排除没有数据的分区
     * @param partitionedRDDWithPartitionID
     * @return
     */
    public static JavaRDD<Tuple2<Integer,Point3D>> partitionsPruning(JavaPairRDD<Integer,Point3D> partitionedRDDWithPartitionID){
        Map<Integer, Boolean> partitionIsEmptyMap = partitionedRDDWithPartitionID.mapPartitionsWithIndex((index, iterator) ->{
            //如果分区为空，则返回false
            if (!iterator.hasNext()){
                return Arrays.asList(new Tuple2<Integer, Boolean>(index, false)).iterator();
            }
            return Arrays.asList(new Tuple2<Integer, Boolean>(index, true)).iterator();
        },true).collect().stream().collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

        class PartitionPruningFunction extends AbstractFunction1<Object, Object> implements Serializable{
            Map<Integer, Boolean> partitionIsEmptyMap;
            public PartitionPruningFunction(Map<Integer, Boolean> partitionIsEmptyMap){
                this.partitionIsEmptyMap = partitionIsEmptyMap;
            }
            @Override
            public Boolean apply(Object v1) {
                return partitionIsEmptyMap.get((int)v1);
            }
        }

        //分区裁剪RDD，传入函数，根据分区id计算出布尔值，true则保留分区，false裁剪分区
        PartitionPruningRDD<Tuple2<Integer,Point3D>> prunedRDDWithOriginalPartitionID = PartitionPruningRDD.create(partitionedRDDWithPartitionID.rdd(),
                new PartitionPruningFunction(partitionIsEmptyMap));

        return prunedRDDWithOriginalPartitionID.toJavaRDD();
    }


}
