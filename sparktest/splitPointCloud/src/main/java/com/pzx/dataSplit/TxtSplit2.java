package com.pzx.dataSplit;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.pzx.BulkLoad;
import com.pzx.DataImport;
import com.pzx.HBaseUtils;
import com.pzx.IOUtils;
import com.pzx.geometry.*;
import com.pzx.pointCloud.HrcFile;
import com.pzx.pointCloud.PointAttribute;
import com.pzx.pointCloud.PointCloud;
import com.pzx.spatialPartition.CustomPartitioner;
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
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;
import scala.runtime.AbstractFunction1;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 根据正方体网格和八叉树进行点云LOD构建和数据分片
 * 数据八叉树分区
 */
public class TxtSplit2 extends TxtSplitter {

    /**
     * 目前存在两大问题  gc时间过长   java数据结构使用过多 占用内存过大
     * 数据分区不均匀  导致某些Task时间过长
     */

    private static Logger logger = Logger.getLogger(TxtSplit2.class);


    private String tableName;

    public TxtSplit2(String inputDirPath, String outputDirPath, String tableName) {
        super(inputDirPath, outputDirPath);
        this.tableName = tableName;
    }


    @Override
    protected void splitData() {
        //空间分区
        JavaPairRDD<Integer, Point3D> partitionedRDDWithPartitionID = spatialPartitioning();
        //分区裁剪
        JavaPairRDD<Integer,Point3D> prunedRDDWithOriginalPartitionID = partitionsPruning(partitionedRDDWithPartitionID);
        //网格筛选
        nodeElementsRDD = shardToNode(prunedRDDWithOriginalPartitionID);
        //直接插入数据库
        bulkLoad(tableName);
    }

    @Override
    protected void createHrc() {
        Map<String, Integer> nodeElementsMap = nodeElementsRDD
                .mapToPair(tuple->new Tuple2<String, Integer>(tuple._1, tuple._2.size()))
                .collectAsMap();

        HrcFile.createHrcFileWithElementsNum(nodeElementsMap, outputDirPath);
    }



    /**
     * 利用八叉树对RDD进行分区

     * @return
     */
    private JavaPairRDD<Integer,Point3D> spatialPartitioning(){

        partitioner = getPartitioner(rowDataSet, sampleFraction);

        JavaRDD<Point3D> pointJavaRDD = rowDataSet.map((MapFunction<Row, Point3D>) row->rowToPoint3D(row), Encoders.kryo(Point3D.class)).toJavaRDD();

        JavaPairRDD<Integer,Point3D> partitionedRDDWithPartitionID = pointJavaRDD.mapPartitionsToPair(pointIterator -> {
            List<Tuple2<Integer, Point3D>> result = new ArrayList<>();
            while (pointIterator.hasNext()){
                Point3D point3D = pointIterator.next();
                int partitionID = partitioner.findPartitionIDs(point3D).get(0);
                result.add(new Tuple2<Integer, Point3D>(partitionID, point3D));
            }
            return result.iterator();
        }).partitionBy(partitioner);

        return partitionedRDDWithPartitionID;
    }





    /*
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



        //创建cloud.js文件
        PointCloud pointCloud = new TxtSplit1().createCloudJS(cachedDataSet,outputDirPath);
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


     */










}
