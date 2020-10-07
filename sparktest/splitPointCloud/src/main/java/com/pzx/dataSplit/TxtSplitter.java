package com.pzx.dataSplit;

import com.alibaba.fastjson.JSONObject;
import com.pzx.BulkLoad;
import com.pzx.IOUtils;
import com.pzx.distributedLock.DistributedRedisLock;
import com.pzx.geometry.Cube;
import com.pzx.geometry.Cuboid;
import com.pzx.geometry.Grid3D;
import com.pzx.geometry.Point3D;
import com.pzx.pointCloud.PointAttribute;
import com.pzx.pointCloud.PointCloud;
import com.pzx.spatialPartition.CustomPartitioner;
import com.pzx.spatialPartition.OcTreePartitioner;
import com.pzx.spatialPartition.OcTreePartitioning;
import com.pzx.utils.SparkUtils;
import com.pzx.utils.SplitUtils;
import org.apache.commons.io.Charsets;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.PartitionPruningRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;
import scala.runtime.AbstractFunction1;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

/**
 * 针对某特定文本格式的读取
 */
public abstract class TxtSplitter extends DataSplitter {

    protected Dataset<Row> rowDataSet;

    protected JavaPairRDD<String, List<byte[]>> nodeElementsRDD;

    protected final double sampleFraction = 0.01;//百分之一

    protected CustomPartitioner partitioner;

    public TxtSplitter(String inputDirPath, String outputDirPath) {
        super(inputDirPath, outputDirPath);
    }

    @Override
    protected void sparkSessionInit() {
        sparkSession = SparkUtils.sparkSessionInit();
    }

    @Override
    protected void loadData() {
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
        rowDataSet = sparkSession.read()
                .format("csv")
                .option("sep"," ")
                .schema(scheme) //.option("inferSchema","true") 模式推理会导致源数据被加载两遍
                .load(inputDirPath)
                .selectExpr("x","y","z","r","g","b");
        rowDataSet.persist(StorageLevel.MEMORY_AND_DISK_SER());

    }

    @Override
    protected void createCloudJS() {
        Dataset<Row> cloudJSDataSet  = rowDataSet.select(min("x"),min("y"),min("z"),
                max("x"),max("y"),max("z"),count(col("x")));

        Row cloudJSRow = cloudJSDataSet.collectAsList().get(0);
        long points = cloudJSRow.getLong(6);
        Cuboid tightBoundingBox =  Cuboid.createFromMinAndMaxCoordinate(cloudJSRow.getDouble(0),cloudJSRow.getDouble(1),cloudJSRow.getDouble(2),
                cloudJSRow.getDouble(3),cloudJSRow.getDouble(4),cloudJSRow.getDouble(5));

        double[] scales = new double[]{0.001,0.001,0.001};

        List<PointAttribute> pointAttributes = Arrays.asList(PointAttribute.POSITION_XYZ, PointAttribute.RGB) ;

        pointCloud = new PointCloud(points,tightBoundingBox,pointAttributes,scales);
        JSONObject cloudJS = pointCloud.buildCloudJS();
        IOUtils.writerDataToFile(outputDirPath + File.separator + CLOUD_JS_FILENAME,cloudJS.toJSONString().getBytes(Charsets.UTF_8),false);
    }

    protected    Point3D rowToPoint3D(Row row){
        double x = (double)row.getAs("x");
        double y = (double)row.getAs("y");
        double z = (double)row.getAs("z");
        int r = (int)row.getAs("r");
        int g = (int)row.getAs("g");
        int b = (int)row.getAs("b");
        return new Point3D(x,y,z,(byte)r,(byte)g,(byte)b);
    }

    protected CustomPartitioner getPartitioner(Dataset<Row> rowDataSet, double sampleFraction){
        //抽样
        List<Point3D> samples = rowDataSet.sample(false,sampleFraction)
                .collectAsList().stream().map(row->rowToPoint3D(row)).collect(Collectors.toList());
        ;
        int partitionNum = rowDataSet.rdd().getNumPartitions();
        Cube boundingBox = pointCloud.getBoundingBox();
        //将分区范围扩大一点点，避免因浮点数精度问题，导致与边界重合的点不在范围内
        //传入正方体范围以便后面的网格处理
        Cuboid partitionsTotalRegion = boundingBox.expandLittle();

        OcTreePartitioning ocTreePartitioning = new OcTreePartitioning(samples, partitionsTotalRegion, partitionNum);
        OcTreePartitioner ocTreePartitioner = ocTreePartitioning.getPartitioner();
        return ocTreePartitioner;
    }

    /**
     * 分区裁剪，排除没有数据的分区
     * @param partitionedRDDWithPartitionID
     * @return
     */
    protected static <U, T> JavaPairRDD<U,T> partitionsPruning(JavaPairRDD<U,T> partitionedRDDWithPartitionID){
        //收集非空的分区索引集合
        Set<Integer> notEmptyPartitionSet = partitionedRDDWithPartitionID.mapPartitionsWithIndex((index, iterator) ->{
            if (iterator.hasNext()){
                return Arrays.asList(index).iterator();
            }
            return Collections.emptyIterator();
        },true).collect().stream().collect(Collectors.toSet());

        class PartitionPruningFunction extends AbstractFunction1<Object, Object> implements Serializable {
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
        //分区裁剪之后，分区的编号会变,所以还是保留之前的partitionid
        PartitionPruningRDD<Tuple2<U,T>> prunedRDDWithOriginalPartitionID = PartitionPruningRDD.create(partitionedRDDWithPartitionID.rdd(),
                new PartitionPruningFunction(notEmptyPartitionSet));

        return prunedRDDWithOriginalPartitionID.toJavaRDD().mapToPair( tuple -> tuple);
    }

    /**
     * 将点数据划分到八叉树节点中
     * @param prunedRDDWithOriginalPartitionID
     * @return
     */
    protected JavaPairRDD<String, List<byte[]>> shardToNode(JavaPairRDD<Integer,Point3D> prunedRDDWithOriginalPartitionID){

        List<Cuboid> partitionRegions = partitioner.getPartitionRegions();
        Cube partitionsTotalRegion = partitioner.getTotalRegion().toCube();
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
                Point3D point3D = iterator.next()._2;
                grid3D.insert(point3D);
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

    protected void bulkLoad(String tableName) {
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
            throw new RuntimeException("bulkLoad data to HBase failed: " + e);
        }

    }

    /**
     * 将每个节点分片的数据写到磁盘上
     */
    protected void writeNode(){
        nodeElementsRDD.foreach(tuple ->{
            String nodeKey = tuple._1;
            String outputFilePath = outputDirPath+File.separator+(nodeKey.length()-1)+nodeKey+".bin";
            IOUtils.writerDataToFile(outputFilePath,tuple._2.iterator(),true);
        });
    }


}
