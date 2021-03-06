package com.pzx.dataSplit;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.pzx.IOUtils;
import com.pzx.distributedLock.DistributedRedisLock;
import com.pzx.geometry.Cube;
import com.pzx.geometry.Cuboid;
import com.pzx.geometry.Point3D;
import com.pzx.pointCloud.HrcFile;
import com.pzx.pointCloud.PointAttribute;
import com.pzx.pointCloud.PointCloud;
import com.pzx.utils.SparkUtils;
import com.pzx.utils.SplitUtils;
import org.apache.commons.io.Charsets;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

/**
 * 直接用CLOD公式和八叉树进行点云LOD构建和数据分片
 */
public class TxtSplit1 extends TxtSplitter {

    private static Logger logger = Logger.getLogger(TxtSplit1.class);
    private static int pointNumPerNode = 30000;
    private static int dimension = 3;

    public TxtSplit1(String inputDirPath, String outputDirPath) {
        super(inputDirPath, outputDirPath);
    }

    @Override
    protected void sparkSessionInit() {
        sparkSession = SparkUtils.sparkSessionInit();
    }

    @Override
    protected void splitData() {
        //广播变量
        Cube boundingBox = pointCloud.getBoundingBox();
        double[] scale = pointCloud.getScales();
        //如果tightBoundingBox某一边小于其他边10倍的话，采用四叉树分片
        Cuboid tightBoundingBox = pointCloud.getTightBoundingBox();

        final int splitDimension = getSplitDimension(tightBoundingBox, boundingBox);
        int maxLevel = SplitUtils.getMaxLevel(pointCloud.getPoints(),pointNumPerNode,splitDimension);

        logger.info("----------------------------------------此次分片的维度为："+splitDimension);
        logger.info("----------------------------------------此次分片的最大层级为："+maxLevel);

        //根据点坐标获取八叉树节点编码
        UserDefinedFunction getNodeKey = getNodeKeyFunction(maxLevel, splitDimension, boundingBox);
        sparkSession.udf().register("getNodeKey", getNodeKey);

        rowDataSet.withColumn("nodeKey",getNodeKey.apply(col("x"),col("y"),col("z")))
                .foreachPartition((ForeachPartitionFunction <Row>)(iterator)->{
                    Map<String,ArrayList<byte[]>> buffer = new HashMap<>();
                    while (iterator.hasNext()){
                        Row row = iterator.next();
                        String nodeKey = row.getAs("nodeKey");
                        buffer.putIfAbsent(nodeKey,new ArrayList());

                        double x = row.getDouble(0);
                        double y = row.getDouble(1);
                        double z = row.getDouble(2);
                        byte r = (byte)row.getInt(3);
                        byte g = (byte)row.getInt(4);
                        byte b = (byte)row.getInt(5);

                        double[] xyzOffset = SplitUtils.getXYZOffset(nodeKey,boundingBox);
                        int newX = (int)((x-xyzOffset[0])/scale[0]);
                        int newY = (int)((y-xyzOffset[1])/scale[1]);
                        int newZ = (int)((z-xyzOffset[2])/scale[2]);
                        byte[] coordinateBytes = SplitUtils.pointInZigZagFormat(new int[]{newX,newY,newZ});

                        List<byte[]> nodeKeyBuffer = buffer.get(nodeKey);
                        nodeKeyBuffer.add(coordinateBytes);
                        nodeKeyBuffer.add(new byte[]{r,g,b});
                    }

                    buffer.forEach((nodekey,list)->{
                        DistributedRedisLock.lock(nodekey);
                        String outputFilePath = outputDirPath+File.separator+(nodekey.length()-1)+nodekey+".bin";
                        IOUtils.writerDataToFile(outputFilePath,list.iterator(),true);
                        DistributedRedisLock.unlock(nodekey);
                    });

                });
    }

    @Override
    protected void createHrc() {
        //创建索引文件
        HrcFile.createHrcFile(outputDirPath);
    }

    //如果tightBoundingBox某一边小于其他边10倍的话，采用四叉树分片
    private int getSplitDimension(Cuboid tightBoundingBox, Cube boundingBox){
        if((tightBoundingBox.getMaxX()-tightBoundingBox.getMinX())<(boundingBox.getMaxX()-boundingBox.getMinX())/3.0||
                (tightBoundingBox.getMaxY()-tightBoundingBox.getMinY())<(boundingBox.getMaxY()-boundingBox.getMinY())/3.0||
                (tightBoundingBox.getMaxZ()-tightBoundingBox.getMinZ())<(boundingBox.getMaxZ()-boundingBox.getMinZ())/3.0){
            return  2;
        }
        return 3;
    }

    //根据点坐标获取八叉树节点编码
    private UserDefinedFunction getNodeKeyFunction(int maxLevel, int splitDimension, Cube boundingBox ){
        return udf(new UDF3<Double,Double,Double,String>() {
            @Override
            public String call(Double x, Double y, Double z) throws Exception {

                double clod = SplitUtils.getClod(maxLevel,splitDimension);
                String nodeKey = SplitUtils.getOctreeNodeName(x,y,z,boundingBox,clod);
                return nodeKey;
            }
        }, DataTypes.StringType);
    }

    /*
    public void main(String[] args) {

        Preconditions.checkArgument(args.length==2,"inputDirPath and outputDirPath is needed！");

        //输入本地输入路径
        String inputDirPath = args[0];
        //生成结果输出路径
        String outputDirPath = args[1];
        //初始化SparkSession
        sparkSession = SparkUtils.sparkSessionInit();


        long time = System.currentTimeMillis();

        //读取数据
        Dataset<Row> dataset = sparkSession.read()
                .format("csv")
                .option("sep"," ")
                .option("inferSchema","true")
                .load(inputDirPath)
                .toDF("x","y","z","intensity","r","g","b")
                .selectExpr("x","y","z","r","g","b");
        dataset.persist(StorageLevel.MEMORY_AND_DISK_SER());

        //创建cloud.js文件
        PointCloud pointCloud = createCloudJS(dataset,outputDirPath);
        logger.info("-----------------------------------生成点云信息文件cloud.js");

        //切分点云
        splitPointCloud(dataset,pointCloud,outputDirPath);
        logger.info("-----------------------------------点云分片任务完成，bin文件全部生成");

        //创建索引文件
        HrcFile.createHrcFile(outputDirPath);
        logger.info("-----------------------------------生成索引文件r.hrc");
        logger.info("-----------------------------------此次点云分片任务全部耗时为："+(System.currentTimeMillis()-time));

        sparkSession.close();

    }

    public PointCloud createCloudJS(Dataset<Row> dataset, String  outputDirPath){


        Dataset<Row> cloudJSDataSet  = dataset.select(min("x"),min("y"),min("z"),
                max("x"),max("y"),max("z"),count(col("x")));

        Row cloudJSRow = cloudJSDataSet.collectAsList().get(0);
        long points = cloudJSRow.getLong(6);
        Cuboid tightBoundingBox =  new Cuboid(cloudJSRow.getDouble(0),cloudJSRow.getDouble(1),cloudJSRow.getDouble(2),
                cloudJSRow.getDouble(3),cloudJSRow.getDouble(4),cloudJSRow.getDouble(5));

        double[] scales = new double[]{0.001,0.001,0.001};

        List<PointAttribute> pointAttributes = Arrays.asList(PointAttribute.POSITION_XYZ, PointAttribute.RGB) ;
        PointCloud pointCloud = new PointCloud(points,tightBoundingBox,pointAttributes,scales);
        JSONObject cloudJS = pointCloud.buildCloudJS();

        IOUtils.writerDataToFile(outputDirPath+ File.separator+"cloud.js",cloudJS.toJSONString().getBytes(Charsets.UTF_8),false);
        return pointCloud;

    }

    public void splitPointCloud(Dataset<Row> dataset, PointCloud pointCloud,String outputDirPath){

        //广播变量
        Cube boundingBox = pointCloud.getBoundingBox();
        double[] scale = pointCloud.getScales();

        //如果tightBoundingBox某一边小于其他边10倍的话，采用四叉树分片
        Cuboid tightBoundingBox = pointCloud.getTightBoundingBox();


        if((tightBoundingBox.getMaxX()-tightBoundingBox.getMinX())<(boundingBox.getMaxX()-boundingBox.getMinX())/3.0||
                (tightBoundingBox.getMaxY()-tightBoundingBox.getMinY())<(boundingBox.getMaxY()-boundingBox.getMinY())/3.0||
                (tightBoundingBox.getMaxZ()-tightBoundingBox.getMinZ())<(boundingBox.getMaxZ()-boundingBox.getMinZ())/3.0){
            dimension = 2;
        }
        final int splitDimension = dimension;
        logger.info("----------------------------------------此次分片的维度为："+splitDimension);
        int maxLevel = SplitUtils.getMaxLevel(pointCloud.getPoints(),pointNumPerNode,splitDimension);
        logger.info("----------------------------------------此次分片的最大层级为："+maxLevel);

        //根据点坐标获取八叉树节点编码
        UserDefinedFunction getNodekey = udf(new UDF3<Double,Double,Double,String>() {
            @Override
            public String call(Double x, Double y, Double z) throws Exception {

                double clod = SplitUtils.getClod(maxLevel,splitDimension);
                String nodeKey = SplitUtils.getOctreeNodeName(x,y,z,boundingBox,clod);
                return nodeKey;
            }
        }, DataTypes.StringType);

        sparkSession.udf().register("getNodekey", getNodekey);

        dataset.withColumn("nodekey",getNodekey.apply(col("x"),col("y"),col("z")))
                .foreachPartition((ForeachPartitionFunction <Row>)(iterator)->{
                    Map<String,ArrayList<byte[]>> buffer = new HashMap<>();
                    while (iterator.hasNext()){
                        Row row = iterator.next();
                        String nodeKey = row.getAs("nodekey");
                        buffer.putIfAbsent(nodeKey,new ArrayList());

                        double x = row.getDouble(0);
                        double y = row.getDouble(1);
                        double z = row.getDouble(2);
                        byte r = (byte)row.getInt(3);
                        byte g = (byte)row.getInt(4);
                        byte b = (byte)row.getInt(5);

                        double[] xyzOffset = SplitUtils.getXYZOffset(nodeKey,boundingBox);
                        int newX = (int)((x-xyzOffset[0])/scale[0]);
                        int newY = (int)((y-xyzOffset[1])/scale[1]);
                        int newZ = (int)((z-xyzOffset[2])/scale[2]);
                        byte[] coordinateBytes = SplitUtils.pointInZigZagFormat(new int[]{newX,newY,newZ});


                        List<byte[]> nodeKeyBuffer = buffer.get(nodeKey);
                        nodeKeyBuffer.add(coordinateBytes);
                        nodeKeyBuffer.add(new byte[]{r,g,b});
                    }

                    buffer.forEach((nodekey,list)->{
                        DistributedRedisLock.lock(nodekey);
                        String outputFilePath = outputDirPath+File.separator+(nodekey.length()-1)+nodekey+".bin";
                        IOUtils.writerDataToFile(outputFilePath,list.iterator(),true);
                        DistributedRedisLock.unlock(nodekey);
                    });

                });
    }
    */






}
