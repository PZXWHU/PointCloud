package com.pzx.dataSplit;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.pzx.BulkLoad;
import com.pzx.DataImport;
import com.pzx.HBaseUtils;
import com.pzx.IOUtils;
import com.pzx.geometry.*;
import com.pzx.pointCloud.CreatePointCloudUDAF;
import com.pzx.pointCloud.HrcFile;
import com.pzx.pointCloud.PointAttribute;
import com.pzx.pointCloud.PointCloud;
import com.pzx.spatialPartition.OcTreePartitioner;
import com.pzx.spatialPartition.OcTreePartitioning;
import com.pzx.utils.SparkUtils;
import com.pzx.utils.SplitUtils;
import jodd.inex.InExRuleMatcher;
import jodd.util.ArraysUtil;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.RangePartitioner;
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
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



    public static void main(String[] args) throws Exception {

        Preconditions.checkArgument(args.length==3,"inputDirPath 、outputDirPath and tableName is needed！");

        //输入本地输入路径
        String inputDirPath = args[0];
        //生成结果输出路径
        String outputDirPath = args[1];
        //生成的表名称
        String tableName = args[2];

        //初始化SparkSession
        sparkSession = SparkUtils.sparkSessionInit();

        long totalTime = System.currentTimeMillis();
        long time = System.currentTimeMillis();
        logger.info("-----------------------------------此次分片任务开始");

        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("x", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("y", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("z", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("intensity", DataTypes.IntegerType, false));//此处使用ByteType并没有太大关系，其不会造成同行的所有列变为ull，因为谓词下推功能，此列数据实际上并没有被解析。
        fields.add(DataTypes.createStructField("r", DataTypes.IntegerType, false));//此处使用IntegerType是因为如果使用ByteType其只能解析范围在[-128,127]内的整数，对于大于127的整数解析为null，并且会造成同行所有的列都被解析为null；
        fields.add(DataTypes.createStructField("g", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("b", DataTypes.IntegerType, false));
        StructType scheme = DataTypes.createStructType(fields);

        //读取数据
        Dataset<Row> originDataset = sparkSession.read()
                .format("csv")
                .option("sep"," ")
                .schema(scheme) //.option("inferSchema","true") 模式推理会导致源数据被加载两遍
                .load(inputDirPath)
                .selectExpr("x","y","z","r","g","b");

        Dataset<Row> cachedDataSet = originDataset.persist(StorageLevel.MEMORY_AND_DISK_SER());

        //System.out.println(cachedDataSet.where("r < 128").where("g<128").where("b<128").count());

        //创建cloud.js文件
        PointCloud pointCloud = TxtSplit1.createCloudJS(cachedDataSet,outputDirPath);
        logger.info("-----------------------------------生成点云信息文件cloud.js, 耗时："+(System.currentTimeMillis()-time));

        time = System.currentTimeMillis();
        //切分点云
        List<Tuple2<String, Integer>> nodeElementsNumTupleList = splitPointCloud(cachedDataSet,pointCloud,tableName);
        logger.info("-----------------------------------点云分片任务完成，bin文件全部生成, 耗时："+(System.currentTimeMillis()-time));

        time = System.currentTimeMillis();
        //创建索引文件
        HrcFile.createHrcFileWithElementsNum(nodeElementsNumTupleList, outputDirPath);
        logger.info("-----------------------------------生成索引文件r.hrc, 耗时："+(System.currentTimeMillis()-time));

        logger.info("-----------------------------------此次点云分片任务全部耗时为："+(System.currentTimeMillis()-totalTime));

        DataImport.file2HBase(outputDirPath, tableName, HBaseUtils.getConnection());

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

        //有类型限制的聚合
        CreatePointCloudUDAF createPointCloudUDAF = new CreatePointCloudUDAF();
        TypedColumn<Point3D, PointCloud> aggPointCloud = createPointCloudUDAF.toColumn();
        Dataset<PointCloud> pointCloudDataSet  = point3DDataset.select(aggPointCloud);
        PointCloud pointCloud = pointCloudDataSet.collectAsList().get(0);

        JSONObject cloudJS = pointCloud.buildCloudJS();
        IOUtils.writerDataToFile(outputDirPath+ File.separator+"cloud.js",cloudJS.toJSONString().getBytes(Charsets.UTF_8),false);
        return pointCloud;

    }



    public static List<Tuple2<String, Integer>> splitPointCloud(Dataset<Row> rowDataset, PointCloud pointCloud,String tableName){

        Tuple2<JavaPairRDD<Integer, Point3D>, OcTreePartitioner> partitionedResultTuple = spatialPartitioning(rowDataset,pointCloud);
        JavaPairRDD<Integer, Point3D> partitionedRDDWithPartitionID = partitionedResultTuple._1;
        OcTreePartitioner ocTreePartitioner = partitionedResultTuple._2;

        JavaRDD<Tuple2<Integer,Point3D>> prunedRDDWithOriginalPartitionID = partitionsPruning(partitionedRDDWithPartitionID);

        JavaPairRDD<String, List<byte[]>> nodeElementsRDD = shardToNode(prunedRDDWithOriginalPartitionID, ocTreePartitioner, pointCloud);

        bulkLoad(nodeElementsRDD, tableName);

        List<Tuple2<String, Integer>> nodeElementsNumTupleList = nodeElementsRDD.mapToPair(tuple->new Tuple2<String, Integer>(tuple._1, tuple._2.size())).collect();

        return nodeElementsNumTupleList;
    }

    public static Point3D rowToPoint3D(Row row){
        double x = (double)row.getAs("x");
        double y = (double)row.getAs("y");
        double z = (double)row.getAs("z");
        int r = (int)row.getAs("r");
        int g = (int)row.getAs("g");
        int b = (int)row.getAs("b");
        return new Point3D(x,y,z,(byte)r,(byte)g,(byte)b);
    }

    /**
     * 利用八叉树对RDD进行分区
     * @param rowDataset
     * @param pointCloud
     * @return
     */
    public static Tuple2<JavaPairRDD<Integer,Point3D>, OcTreePartitioner> spatialPartitioning(Dataset<Row> rowDataset, PointCloud pointCloud){

        double sampleFraction = 0.01;//百分之一
        List<Point3D> samples = rowDataset.sample(false,sampleFraction)
                .collectAsList().stream().map(row->rowToPoint3D(row)).collect(Collectors.toList());

        JavaRDD<Point3D> point3DJavaRDD = rowDataset.map((MapFunction<Row, Point3D>) row->rowToPoint3D(row),Encoders.kryo(Point3D.class)).toJavaRDD();

        int partitionNum = point3DJavaRDD.partitions().size();
        Cube boundingBox = pointCloud.getBoundingBox();
        //将分区范围扩大一点点，避免因浮点数精度问题，导致与边界重合的点不在范围内
        //传入正方体范围以便后面的网格处理
        Cube partitionsTotalRegion = (Cube) boundingBox.expandLittle();

        OcTreePartitioning ocTreePartitioning = new OcTreePartitioning(samples, partitionsTotalRegion,partitionNum);
        OcTreePartitioner ocTreePartitioner = ocTreePartitioning.getPartitioner();

        JavaPairRDD<Integer,Point3D> partitionedRDDWithPartitionID = point3DJavaRDD.mapPartitionsToPair(pointIterator -> {

            List<Tuple2<Integer, Point3D>> result = new ArrayList<>();
            while (pointIterator.hasNext()){
                Point3D point3D = pointIterator.next();
                int partitionID = ocTreePartitioner.findPartitionIDForObject(point3D);
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
        Set<Integer> notEmptyPartitionSet = partitionedRDDWithPartitionID.mapPartitionsWithIndex((index, iterator) ->{
            if (iterator.hasNext()){
                return Arrays.asList(index).iterator();
            }
            return Collections.emptyIterator();
        },true).collect().stream().collect(Collectors.toSet());

        class PartitionPruningFunction extends AbstractFunction1<Object, Object> implements Serializable{
            Set<Integer> notEmptyPartitionSet;
            public PartitionPruningFunction(Set<Integer> notEmptyPartitionSet){
                this.notEmptyPartitionSet = notEmptyPartitionSet;
            }
            @Override
            public Boolean apply(Object v1) {
                return notEmptyPartitionSet.contains((Integer)v1);
            }
        }

        //分区裁剪RDD，传入函数，根据分区id计算出布尔值，true则保留分区，false裁剪分区
        PartitionPruningRDD<Tuple2<Integer,Point3D>> prunedRDDWithOriginalPartitionID = PartitionPruningRDD.create(partitionedRDDWithPartitionID.rdd(),
                new PartitionPruningFunction(notEmptyPartitionSet));

        return prunedRDDWithOriginalPartitionID.toJavaRDD();
    }

    /**
     * 将点数据划分到八叉树节点中
     * @param prunedRDDWithOriginalPartitionID
     * @return
     */
    public static JavaPairRDD<String, List<byte[]>> shardToNode(JavaRDD<Tuple2<Integer,Point3D>> prunedRDDWithOriginalPartitionID,
                                                                OcTreePartitioner ocTreePartitioner,PointCloud pointCloud){

        List<Cuboid> partitionRegions = ocTreePartitioner.getPartitionRegions();
        Cube partitionsTotalRegion = ocTreePartitioner.getPartitionsTotalRegions().toBoundingBoxCube();
        //初始网格一个坐标轴的单元数,网格单元边长
        int initGridOneSideCellNum = 1 << 5;
        double initGridCellSideLength = partitionsTotalRegion.getXSideLength() / initGridOneSideCellNum;
        double[] coordinatesScale = pointCloud.getScales();

        //广播变量
        Broadcast<List<Cuboid>> partitionRegionsBroadcast = sparkSession.sparkContext().broadcast(partitionRegions, ClassManifestFactory.classType(List.class));

        JavaPairRDD<String, List<byte[]>> nodeElementsRDD = prunedRDDWithOriginalPartitionID.mapPartitionsToPair((Iterator<Tuple2<Integer,Point3D>> iterator) ->{

            Tuple2<Integer, Point3D> pointWithOriginalPartitionID = iterator.next();
            Cuboid partitionRegion = partitionRegionsBroadcast.getValue().get(pointWithOriginalPartitionID._1);
            Grid3D grid3D = new Grid3D(partitionRegion, initGridCellSideLength, partitionsTotalRegion);

            grid3D.insert(pointWithOriginalPartitionID._2);
            while (iterator.hasNext()){
                grid3D.insert(iterator.next()._2);
            }

            HashMap<String, List<byte[]>> nodeElementsMap = grid3D.shardToNode(coordinatesScale);
            List<Tuple2<String, List<byte[]>>> nodeElementsTupleList = SplitUtils.mapToTupleList(nodeElementsMap);

            return nodeElementsTupleList.iterator();
        }, true).reduceByKey((list1, list2)->{
            list1.addAll(list2);
            return list1;
        });

        return nodeElementsRDD;
    }


    public static void bulkLoad(JavaPairRDD<String, List<byte[]>> nodeElementsRDD, String tableName) {
        JavaPairRDD<ImmutableBytesWritable, KeyValue> hFileRDD = nodeElementsRDD.mapToPair(tuple->{
            String nodeKey = tuple._1();
            return new Tuple2<String,List<byte[]>>((nodeKey.length()-1)+ nodeKey, tuple._2);
        }).sortByKey().mapToPair(tuple->{
            String nodeKey = tuple._1;
            List<byte[]> pointsBytesList = tuple._2;
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            for(byte[] pointBytes : pointsBytesList){
                byteArrayOutputStream.write(pointBytes);
            }
            byte[] pointBytesArray = byteArrayOutputStream.toByteArray();

            byte[] rowKey = Bytes.toBytes(nodeKey);
            byte[] columnFamily = Bytes.toBytes("data");
            byte[] columnQualifier = Bytes.toBytes("bin");
            ImmutableBytesWritable immutableRowKey = new ImmutableBytesWritable(rowKey);

            KeyValue keyValue = new KeyValue(rowKey, columnFamily, columnQualifier, pointBytesArray);
            return new Tuple2<ImmutableBytesWritable, KeyValue>(immutableRowKey, keyValue);
        });
        try {
            BulkLoad.bulkLoad(hFileRDD, tableName);
        }catch (Exception e){
            logger.warn(e);
            throw new RuntimeException("bulkLoad data to HBase failed: " + e);
        }

    }

}
