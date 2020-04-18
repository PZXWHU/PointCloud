package com.pzx.dataSplit;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.pzx.IOUtils;
import com.pzx.distributedLock.DistributedRedisLock;
import com.pzx.pointCloud.PointAttribute;
import com.pzx.pointCloud.PointCloud;
import com.pzx.utils.SparkUtils;
import com.pzx.utils.SplitUtils;
import org.apache.commons.io.Charsets;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

/**
 * 直接用CLOD公式和八叉树进行点云LOD构建和数据分片
 */
public class TxtSplit1 {

    private static Logger logger = Logger.getLogger(TxtSplit1.class);
    private static int pointNumPerNode = 30000;
    private static int dimension = 3;
    private static SparkSession sparkSession;

    public static void main(String[] args) {

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
        createHrcFile(outputDirPath);
        logger.info("-----------------------------------生成索引文件r.hrc");
        logger.info("-----------------------------------此次点云分片任务全部耗时为："+(System.currentTimeMillis()-time));

        sparkSession.close();

    }


    /**
     * 创建cloud.js文件
     * @param dataset
     * @param outputDirPath
     * @return
     */
    public static PointCloud createCloudJS(Dataset<Row> dataset, String  outputDirPath){


        Dataset<Row> cloudJSDataSet  = dataset.select(max("x"),max("y"),max("z"),
                min("x"),min("y"),min("z"),count(col("x")));

        Row cloudJSRow = cloudJSDataSet.collectAsList().get(0);
        long points = cloudJSRow.getLong(6);
        double[] tightBoundingBox =  new double[]{cloudJSRow.getDouble(0),cloudJSRow.getDouble(1),cloudJSRow.getDouble(2),
                cloudJSRow.getDouble(3),cloudJSRow.getDouble(4),cloudJSRow.getDouble(5)};

        double[] scales = new double[]{0.001,0.001,0.001};

        List<PointAttribute> pointAttributes = Arrays.asList(PointAttribute.POSITION_XYZ, PointAttribute.RGB) ;
        PointCloud pointCloud = new PointCloud(points,tightBoundingBox,pointAttributes,scales);
        JSONObject cloudJS = pointCloud.buildCloudJS();

        IOUtils.writerDataToFile(outputDirPath+ File.separator+"cloud.js",cloudJS.toJSONString().getBytes(Charsets.UTF_8),false);
        return pointCloud;

    }

    public static void splitPointCloud(Dataset<Row> dataset, PointCloud pointCloud,String outputDirPath){

        //广播变量
        double[] boundingBox = pointCloud.getBoundingBox();
        double[] scale = pointCloud.getScales();

        //如果tightBoundingBox某一边小于其他边10倍的话，采用四叉树分片
        double[] tightBoundingBox = pointCloud.getTightBoundingBox();


        if((tightBoundingBox[0]-tightBoundingBox[3])<(boundingBox[0]-boundingBox[3])/3.0||
                (tightBoundingBox[1]-tightBoundingBox[4])<(boundingBox[0]-boundingBox[3])/3.0||
                (tightBoundingBox[2]-tightBoundingBox[5])<(boundingBox[0]-boundingBox[3])/3.0){
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
                //.groupByKey((Function1<Row, String>) row->row.getAs("nodekey"),Encoders.STRING())
                //.groupBy("nodekey")
                /*.agg(new Aggregator<Row,Row,Double>(){

                    @Override
                    public Double finish(Row reduction) {
                        return null;
                    }

                    @Override
                    public Encoder<Row> bufferEncoder() {
                        return null;
                    }

                    @Override
                    public Row zero() {
                        return null;
                    }

                    @Override
                    public Encoder<Double> outputEncoder() {
                        return null;
                    }

                    @Override
                    public Row reduce(Row b, Row a) {
                        return null;
                    }

                    @Override
                    public Row merge(Row b1, Row b2) {
                        return null;
                    }
                }.toColumn())
                 */
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

    /**
     * 利用输出目录中所有的bin文件的文件名（即nodeKey），生成索引文件
     * @param outputDirPath 输出目录
     */
    public static void createHrcFile(String outputDirPath){

        List<String> binFilePathList = IOUtils.listAllFiles(outputDirPath).stream().filter((x)->{return x.endsWith(".bin");}).collect(Collectors.toList());
        List<String> nodeKeyList = binFilePathList.stream().map((binFilePath)->binFilePath.substring(binFilePath.lastIndexOf(File.separator)+1,binFilePath.lastIndexOf("."))).collect(Collectors.toList());

        byte[] hrcBytes = createHrcBytes(nodeKeyList);

        try {
            IOUtils.writerDataToFile(outputDirPath+File.separator+"r.hrc",hrcBytes,false);
        }catch (Exception e){
            e.printStackTrace();
        }

    }


    public static byte[] createHrcBytes(List<String> nodeKeyList){
        HashSet<String> nodeKeySet = (HashSet<String>) nodeKeyList.stream().collect(Collectors.toSet());

        nodeKeyList.sort(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                if(o1.length()>o2.length())
                    return 1;
                else if (o1.length()<o2.length())
                    return -1;
                else {
                    return o1.compareTo(o2);
                }
            }
        });
        byte[] hrcBytes = new byte[nodeKeyList.size()];

        for(int i=0;i<nodeKeyList.size();i++){
            String nodeKey = nodeKeyList.get(i);
            byte mask = 0;

            for(int j=0;j<8;j++){
                if(nodeKeySet.contains(nodeKey+j))
                    mask = (byte) (mask|1<<j);
            }
            hrcBytes[i] = mask;
        }

        return hrcBytes;
    }


}
