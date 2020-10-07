package com.pzx.dataSplit;

import com.pzx.HDFSUtils;
import com.pzx.IOUtils;
import com.pzx.distributedLock.DistributedRedisLock;
import com.pzx.geometry.Cube;
import com.pzx.geometry.Cuboid;
import com.pzx.lasFile.LasFile;
import com.pzx.lasFile.LasFileHeader;
import com.pzx.lasFile.LasFilePointData;
import com.pzx.pointCloud.HrcFile;
import com.pzx.pointCloud.PointAttribute;
import com.pzx.pointCloud.PointCloud;
import com.pzx.utils.LittleEndianUtils;

import com.pzx.utils.SparkUtils;
import com.pzx.utils.SplitUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * 利用CLOD公式和八叉树进行LOD构建和数据分片
 * 微批量流式处理
 */
public class LasSplit {

    private static Logger logger = Logger.getLogger(LasSplit.class);

    private static long pointBatchLimit = 30000000;
    private static int pointNumPerNode = 30000;
    private static int dimension = 3;


    public static void main(String[] args) {
        //输入本地输入路径
        String inputDirPath = "C:\\Users\\PZX\\Desktop\\新建文件夹";
        //生成结果输出路径
        String outputDirPath = "C:\\Users\\PZX\\Desktop\\新建文件夹\\result";
        if(args.length==2){

            inputDirPath = args[0];
            outputDirPath = args[1];
        }

        long time = System.currentTimeMillis();
        List<String> lasFilePathList = IOUtils.listAllFiles(inputDirPath).stream().filter((x)->{return x.endsWith(".las");}).collect(Collectors.toList());

        PointCloud pointCloud = createCloudJS(lasFilePathList,outputDirPath);
        logger.info("-----------------------------------生成点云信息文件cloud.js");

        splitPointCloud(lasFilePathList,pointCloud,outputDirPath);
        logger.info("-----------------------------------点云分片任务完成，bin文件全部生成");


        HrcFile.createHrcFile(outputDirPath);
        //createHrcRow(tableName);
        logger.info("-----------------------------------生成索引文件r.hrc");
        logger.info("-----------------------------------此次点云分片任务全部耗时为："+(System.currentTimeMillis()-time));

    }


    /**
     * 生成点云信息cloud.js文件
     * @param lasFilePathList 输入目录下所有las文件的文件路径
     * @param outputDirPath 输出路径，cloud.js文件将写到此目录下
     * @return 点云信息cloud.js (JSONObject)
     */
    public static PointCloud createCloudJS(List<String> lasFilePathList,String outputDirPath){
        PointCloud pointCloud = getPointCloudInformation(lasFilePathList);
        try {
            //HBaseUtils.put(tableName,"cloud","data","js",cloudJS.toJSONString().getBytes("utf-8"));
            IOUtils.writerDataToFile(outputDirPath+ File.separator+"cloud.js",pointCloud.buildCloudJS().toJSONString().getBytes("utf-8"),false);
        }catch (Exception e){
            e.printStackTrace();
            logger.warn("cloud.js文件生成失败");
        }
        return pointCloud;
    }


    /**
     * 获取点云信息cloud.js (JSONObject)
     * @param lasFilePathList 输入目录下所有las文件的文件路径
     * @return 点云信息cloud.js (JSONObject)
     */
    private static PointCloud getPointCloudInformation(List<String> lasFilePathList){
        long points = 0L;
        double[] scale = new double[]{1,1,1};
        Cuboid tightBoundingBox = Cuboid.createFromMinAndMaxCoordinate(Double.MAX_VALUE,Double.MAX_VALUE,Double.MAX_VALUE,
                -Double.MIN_VALUE,-Double.MIN_VALUE,-Double.MIN_VALUE);
        double[] boundingBox;

        for(String lasFilePath:lasFilePathList){
            long time = System.currentTimeMillis();
            LasFile lasFile = new LasFile(lasFilePath);
            LasFileHeader lasFileHeader = lasFile.getLasFileHeader();
            //点数量
            points += lasFileHeader.getNumberOfPointRecords();
            //点xyzScale
            double[] lasScale = lasFileHeader.getScale();
            for(int i=0;i<3;i++){
                scale[i] = Math.min(scale[i],lasScale[i]);
            }
            //tightBoundingBox
            Cuboid lasTightBounding = lasFileHeader.getBox();
            tightBoundingBox = tightBoundingBox.mergedRegion(lasTightBounding);
        }

        List<PointAttribute> pointAttributes = Arrays.asList(PointAttribute.POSITION_XYZ, PointAttribute.RGB);
        PointCloud pointCloud = new PointCloud(points, tightBoundingBox, pointAttributes,scale );

        return pointCloud;
    }





    /**
     * 点云分片
     * @param lasFilePathList 输入目录下所有las文件的文件路径
     * @param pointCloud 点云总体信息
     * @param outputDirPath 输出目录
     */
    public static void splitPointCloud(List<String> lasFilePathList,PointCloud pointCloud,String outputDirPath){
        JavaSparkContext sc = SparkUtils.sparkContextInit();

        ExecutorService executorService = Executors.newCachedThreadPool();
        long points = pointCloud.getPoints();

        List<CountDownLatch> countDownLatchList = new ArrayList<>();

        //当下一次读取的点数之和将超过pointBytesListLimit 出发一下spark任务
        ByteBuffer pointBuffer = ByteBuffer.allocate(Integer.MAX_VALUE/2);


        for(int i=0;i<lasFilePathList.size();i++){
            String lasFilePath = lasFilePathList.get(i);
            LasFile lasFile = new LasFile(lasFilePath);
            //LasFileHeader lasFileHeader = lasFile.getLasFileHeader();
            List<LasFilePointData> lasFilePointDataList = lasFile.getLasFilePointDataList();


            for(int j=0;j<lasFilePointDataList.size();j++){
                LasFilePointData lasFilePointData = lasFilePointDataList.get(j);

                if (lasFilePointData.getNumberOfPointRecords()*27 + pointBuffer.position() > pointBuffer.capacity()-1||((i==lasFilePathList.size()-1)&&(j==lasFilePointDataList.size()-1))){
                    //如果缓冲区点数超过BatchLimit，则触发一次spark任务

                    CountDownLatch countDownLatch = new CountDownLatch(1);
                    countDownLatchList.add(countDownLatch);
                    final int sparkTaskIndex = countDownLatchList.size();

                    pointBuffer.flip();
                    byte[] tmpFileBytes = new byte[pointBuffer.remaining()];
                    pointBuffer.get(tmpFileBytes,0,tmpFileBytes.length);
                    pointBuffer.clear();

                    //生成中间文件，供spark任务使用
                    long time = System.currentTimeMillis();
                    String tmpFileName = createTmpFile(tmpFileBytes,outputDirPath);
                    logger.info("----------------------------------------------------生成中间文件耗时："+(System.currentTimeMillis()-time));


                    executorService.execute(()->{

                        logger.info("-------------------------------------------------------------提交spark任务"+sparkTaskIndex);
                        doSparkTask(sc,tmpFileName ,pointCloud,outputDirPath);
                        countDownLatch.countDown();
                        logger.info("-------------------------------------------------------------spark任务"+sparkTaskIndex+"结束");
                    });

                }

                lasFilePointData.pointBytesToByteBuffer(pointBuffer);

            }
        }

        try {
            for(CountDownLatch countDownLatch1 :countDownLatchList)
                countDownLatch1.await();
            logger.info("-------------------------------------------------------------全部spark任务完成");
        }catch (Exception e){
            e.printStackTrace();
        }

        //关闭 JavaSparkContext
        sc.close();
    }



    /**
     * 生成中间文件，spark任务使用
     * 中间文件为二进制文件：每27个字节为一个点 xyz分别为8字节double，rgb为1字节byte
     * @param tmpFileBytes 缓冲区字节，用于缓冲内存中的点，到到达一定大小，就生成中间文件写到输出目录中（一般为hdfs）
     * @param outputDirPath 输出目录
     * @return
     */
    public static String createTmpFile(byte[] tmpFileBytes,String outputDirPath){

        String tmpFileName = System.currentTimeMillis()+".tmp";
        IOUtils.writerDataToFile(outputDirPath+File.separator+tmpFileName,tmpFileBytes,false);

        return tmpFileName;


    }


    /**
     * 用中间文件作为spark任务的源数据，触发spark任务
     * spark任务完成，删除中间文件
     * @param sc JavaSparkContext用于触发spark任务
     * @param tmpFileName 中间文件名，spark任务的源数据
     * @param pointCloud 点云的信息，包括包围盒，总点数，用于广播变量（rdd转换使用）
     * @param outputDirPath 输出目录
     */
    public static void doSparkTask(JavaSparkContext sc,String tmpFileName,PointCloud pointCloud,String outputDirPath){

        //广播变量
        Cube boundingBox = pointCloud.getBoundingBox();
        double[] scale = pointCloud.getScales();


        //如果tightBoundingBox某一边小于其他边10倍的话，采用四叉树分片
        Cuboid tightBoundingBox = pointCloud.getTightBoundingBox();

        if((tightBoundingBox.getXSideLength())<(boundingBox.getSideLength())/10.0||
                (tightBoundingBox.getYSideLength())<(boundingBox.getSideLength())/10.0||
                (tightBoundingBox.getZSideLength())<(boundingBox.getSideLength())/10.0){
            dimension = 2;

        }
        final int splitDimension = dimension;
        logger.info("----------------------------------------此次分片的维度为："+splitDimension);
        int maxLevel = SplitUtils.getMaxLevel(pointCloud.getPoints(),pointNumPerNode,splitDimension);
        logger.info("----------------------------------------此次分片的最大层级为为："+maxLevel);

        JavaRDD<byte[]>pointBytesRDD = sc.binaryRecords(outputDirPath+File.separator+tmpFileName,27);

        pointBytesRDD.mapToPair((byte[] pointBytes)->{

            double x = LittleEndianUtils.bytesToDouble(pointBytes[0],pointBytes[1],pointBytes[2],pointBytes[3],pointBytes[4],pointBytes[5],pointBytes[6],pointBytes[7]);
            double y = LittleEndianUtils.bytesToDouble(pointBytes[8],pointBytes[9],pointBytes[10],pointBytes[11],pointBytes[12],pointBytes[13],pointBytes[14],pointBytes[15]);
            double z = LittleEndianUtils.bytesToDouble(pointBytes[16],pointBytes[17],pointBytes[18],pointBytes[19],pointBytes[20],pointBytes[21],pointBytes[22],pointBytes[23]);
            byte r = pointBytes[24];
            byte g = pointBytes[25];
            byte b = pointBytes[26];

            double clod = SplitUtils.getClod(maxLevel,splitDimension);
            String nodeKey = SplitUtils.getOctreeNodeName(x,y,z,boundingBox,clod);
            double[] xyzOffset = SplitUtils.getXYZOffset(nodeKey,boundingBox);
            int newX = (int)((x-xyzOffset[0])/scale[0]);
            int newY = (int)((y-xyzOffset[1])/scale[1]);
            int newZ = (int)((z-xyzOffset[2])/scale[2]);

            byte[] coordinateBytes = SplitUtils.pointInZigZagFormat(new int[]{newX,newY,newZ});
            int coordinateBytesLength = coordinateBytes.length;


            byte[] pointNewBytesArray = new byte[coordinateBytes.length+3];
            for(int i=0;i<coordinateBytesLength;i++){
                pointNewBytesArray[i] = coordinateBytes[i];
            }

            pointNewBytesArray[coordinateBytesLength] = r;
            pointNewBytesArray[coordinateBytesLength+1] = g;
            pointNewBytesArray[coordinateBytesLength+2] = b;

            return new Tuple2<String,byte[]>(nodeKey,pointNewBytesArray);

        }) .combineByKey((byte[] pointBytesArray)->{
            List<byte[]> list = new ArrayList<>();
            list.add(pointBytesArray);
            return list;
        },(List<byte[]> list1,byte[] pointBytesArray)->{
            list1.add(pointBytesArray);
            return list1;
        },(List<byte[]> list1,List<byte[]> list2)->{
            list1.addAll(list2);
            return list1;
        }).foreach((Tuple2<String,List<byte[]>> tuple2)->{
            String nodeKey = tuple2._1;
            Iterator<byte[]> iterator = tuple2._2.iterator();

            //分布式锁
            /**/
            DistributedRedisLock.lock(nodeKey);
            String outputFilePath = outputDirPath+File.separator+(nodeKey.length()-1)+nodeKey+".bin";
            IOUtils.writerDataToFile(outputFilePath,iterator,true);
            DistributedRedisLock.unlock(nodeKey);


        });


        try {
            HDFSUtils.deleteFile(outputDirPath+File.separator+tmpFileName);
        }catch (Exception e){
            e.printStackTrace();
            logger.warn("删除临时文件失败！临时文件位置为："+outputDirPath+File.separator+tmpFileName);
        }


    }




}
