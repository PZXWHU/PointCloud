package com.pzx.split;

import com.alibaba.fastjson.JSONObject;
import com.pzx.HDFSUtils;
import com.pzx.IOUtils;
import com.pzx.distributedLock.DistributedRedisLock;
import com.pzx.las.LasFile;
import com.pzx.las.LasFileHeader;
import com.pzx.las.LasFilePointData;
import com.pzx.utils.LittleEndianUtils;

import com.pzx.utils.CloudJSUtils;
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

        JSONObject cloudjs = createCloudJS(lasFilePathList,outputDirPath);
        logger.info("-----------------------------------生成点云信息文件cloud.js");

        splitPointCloud(lasFilePathList,cloudjs,outputDirPath);
        logger.info("-----------------------------------点云分片任务完成，bin文件全部生成");



        List<String> binFilePathList = IOUtils.listAllFiles(outputDirPath).stream().filter((x)->{return x.endsWith(".bin");}).collect(Collectors.toList());

        createHrcFile(binFilePathList,outputDirPath);
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
    public static JSONObject createCloudJS(List<String> lasFilePathList,String outputDirPath){
        JSONObject cloudJS = getPointCloudInformationJson(lasFilePathList);
        try {
            //HBaseUtils.put(tableName,"cloud","data","js",cloudJS.toJSONString().getBytes("utf-8"));
            IOUtils.writerDataToFile(outputDirPath+ File.separator+"cloud.js",cloudJS.toJSONString().getBytes("utf-8"),false);
        }catch (Exception e){
            e.printStackTrace();
            logger.warn("cloud.js文件生成失败");
        }
        return cloudJS;
    }


    /**
     * 获取点云信息cloud.js (JSONObject)
     * @param lasFilePathList 输入目录下所有las文件的文件路径
     * @return 点云信息cloud.js (JSONObject)
     */
    private static JSONObject getPointCloudInformationJson(List<String> lasFilePathList){
        long points = 0L;
        double[] scale = new double[]{1,1,1};
        double[] tightBoundingBox = new double[]{-Double.MIN_VALUE,-Double.MIN_VALUE,-Double.MIN_VALUE,Double.MAX_VALUE,Double.MAX_VALUE,Double.MAX_VALUE};
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
            double[] lasTightBounding = lasFileHeader.getBox();
            for (int i=0;i<3;i++){
                tightBoundingBox[i] = Math.max(tightBoundingBox[i],lasTightBounding[i]);
                tightBoundingBox[i+3] = Math.min(tightBoundingBox[i+3],lasTightBounding[i+3]);
            }
        }

        boundingBox = CloudJSUtils.getBoundingBox(tightBoundingBox);
        String[] pointAttributes = new String[]{"POSITION_CARTESIAN","RGB_PACKED"};
        JSONObject cloudjs = CloudJSUtils.buildCloudJS(points,tightBoundingBox,boundingBox,scale,pointAttributes);

        return cloudjs;
    }





    /**
     * 点云分片
     * @param lasFilePathList 输入目录下所有las文件的文件路径
     * @param cloudjs 点云信息cloud.js (JSONObject)
     * @param outputDirPath 输出目录
     */
    public static void splitPointCloud(List<String> lasFilePathList,JSONObject cloudjs,String outputDirPath){
        JavaSparkContext sc = SparkUtils.scInit();

        /* 使用pointBytesList
        List<byte[]> pointBytesList = new ArrayList<>();
        //当下一次读取的点数之和将超过pointBytesListLimit 出发一下spark任务
        for(String lasFilePath:lasFilePathList){
            long time = System.currentTimeMillis();
            LasFile lasFile = new LasFile(lasFilePath);
            LasFileHeader lasFileHeader = lasFile.getLasFileHeader();
            long pointCount = lasFileHeader.getNumberOfPointRecords();
            if(pointCount+pointBytesList.size()>pointBytesListLimit){
                doSparkTask(sc,pointBytesList,cloudjs,outputDirPath);
                pointBytesList.clear();
            }else {
                List<byte[]> list = lasFile.getLasFilePointData().getPointBytesList();

                pointBytesList.addAll(list);
            }

        }
        doSparkTask(sc,pointBytesList,cloudjs,outputDirPath);

         */


        ExecutorService executorService = Executors.newCachedThreadPool();
        long points = cloudjs.getLong("points");

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
                        doSparkTask(sc,tmpFileName ,cloudjs,outputDirPath);
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
     * @param cloudjs 点云的信息，包括包围盒，总点数，用于广播变量（rdd转换使用）
     * @param outputDirPath 输出目录
     */
    public static void doSparkTask(JavaSparkContext sc,String tmpFileName,JSONObject cloudjs,String outputDirPath){

        //广播变量
        JSONObject boundingBoxJson = cloudjs.getJSONObject("boundingBox");
        double[] boundingBox = new double[]{boundingBoxJson.getDoubleValue("ux"),boundingBoxJson.getDoubleValue("uy"),boundingBoxJson.getDoubleValue("uz"),
                boundingBoxJson.getDoubleValue("lx"),boundingBoxJson.getDoubleValue("ly"),boundingBoxJson.getDoubleValue("lz")};
        double[] scale = (double[])cloudjs.get("scale");


        //如果tightBoundingBox某一边小于其他边10倍的话，采用四叉树分片
        JSONObject tightBoundingBoxJson = cloudjs.getJSONObject("tightBoundingBox");

        if((tightBoundingBoxJson.getDoubleValue("ux")-tightBoundingBoxJson.getDoubleValue("lx"))<(boundingBox[0]-boundingBox[3])/10.0||
                (tightBoundingBoxJson.getDoubleValue("uy")-tightBoundingBoxJson.getDoubleValue("ly"))<(boundingBox[0]-boundingBox[3])/10.0||
                (tightBoundingBoxJson.getDoubleValue("uz")-tightBoundingBoxJson.getDoubleValue("lz"))<(boundingBox[0]-boundingBox[3])/10.0){
            dimension = 2;

        }
        final int splitDimension = dimension;
        logger.info("----------------------------------------此次分片的维度为："+splitDimension);
        int maxLevel = SplitUtils.getMaxLevel(cloudjs.getLong("points"),pointNumPerNode,splitDimension);
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
            //pointNewBytesArray[coordinateBytesLength+3] = newClodBytes[0];
            //pointNewBytesArray[coordinateBytesLength+4] = newClodBytes[1];

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

            /*
            ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.MAX_VALUE/2);
            while (iterator.hasNext()){
                byte[] pointBytesArray = iterator.next();
                byteBuffer.put(pointBytesArray);
            }
            byteBuffer.flip();
            byte[] pointBytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(pointBytes,0,pointBytes.length);
            byteBuffer.clear();

             */

            //分布式锁
            /**/
            DistributedRedisLock.lock(nodeKey);
            String outputFilePath = outputDirPath+File.separator+(nodeKey.length()-1)+nodeKey+".bin";
            IOUtils.writerDataToFile(outputFilePath,iterator,true);
            DistributedRedisLock.unlock(nodeKey);

            //HBaseUtils.put(tableName,(nodeKey.length()-1)+nodeKey,"data",System.currentTimeMillis()+"",pointBytes);


        });



              /*
                .mapPartitions(iterator ->{
            Map<String,ByteArrayOutputStream>  map = new HashMap<>();
            while (iterator.hasNext()){
                Tuple2<String,byte[]> tuple2 = iterator.next();
                map.putIfAbsent(tuple2._1,new ByteArrayOutputStream());
                map.get(tuple2._1).write(tuple2._2);
            }
            return map.entrySet().iterator();
        }).foreach(entry ->{
            String nodeKey = entry.getKey();
            ByteArrayOutputStream byteArrayOutputStream = entry.getValue();
            //分布式锁
            DistributedRedisLock.lock(nodeKey);

            IOUtils.writerDataToFile(outputDirPath+File.separator+nodeKey+".bin",byteArrayOutputStream.toByteArray(),true);
            DistributedRedisLock.unlock(nodeKey);
            byteArrayOutputStream.close();
        });

               */


        try {
            HDFSUtils.deleteFile(outputDirPath+File.separator+tmpFileName);
        }catch (Exception e){
            e.printStackTrace();
            logger.warn("删除临时文件失败！临时文件位置为："+outputDirPath+File.separator+tmpFileName);
        }


    }


    /**
     * 在内存中使用list进行缓冲，不写入中间文件，直接使用parallelize，分发到spark集群中
     * 此方法在内存中使用list很耗时，从driver中分发到集群也很耗时，所以不使用此方法
     * @param sc JavaSparkContext用于触发spark任务
     * @param pointBytesList 点字节数组，用于spark任务的源数据
     * @param cloudjs 点云的信息，包括包围盒，总点数，用于广播变量（rdd转换使用）
     * @param outputDirPath 输出目录
     *
     */
    /*
    public static void doSparkTask(JavaSparkContext sc,List<byte[]> pointBytesList,JSONObject cloudjs,String outputDirPath){

        //广播变量
        int maxLevel = SplitUtils.getMaxLevel(cloudjs.getLong("points"),pointNumPerNode,dimension);
        JSONObject boundingBoxJson = cloudjs.getJSONObject("boundingBox");
        double[] boundingBox = new double[]{boundingBoxJson.getDoubleValue("ux"),boundingBoxJson.getDoubleValue("uy"),boundingBoxJson.getDoubleValue("uz"),
                boundingBoxJson.getDoubleValue("lx"),boundingBoxJson.getDoubleValue("ly"),boundingBoxJson.getDoubleValue("lz")};
        double[] scale = (double[])cloudjs.get("scale");

        JavaRDD<byte[]> pointBytesRDD = sc.parallelize(pointBytesList);

        pointBytesRDD.mapToPair((byte[] pointByes)->{

            byte[] bytes = new byte[8];
            for(int i=0;i<8;i++){
                bytes[i] = pointByes[i];
            }
            double x  = LittleEndianUtils.bytesToDouble(bytes);
            for(int i=0;i<8;i++){
                bytes[i] = pointByes[8+i];
            }
            double y  = LittleEndianUtils.bytesToDouble(bytes);
            for(int i=0;i<8;i++){
                bytes[i] = pointByes[16+i];
            }
            double z = LittleEndianUtils.bytesToDouble(bytes);

            double clod = SplitUtils.getClod(maxLevel,dimension);
            String nodeKey = SplitUtils.getOctreeNodeName(x,y,z,boundingBox,clod);
            double[] xyzOffset = SplitUtils.getXYZOffset(nodeKey,boundingBox);
            int newX = (int)((x-xyzOffset[0])/scale[0]);
            int newY = (int)((y-xyzOffset[1])/scale[1]);
            int newZ = (int)((z-xyzOffset[2])/scale[2]);
            byte r = pointByes[24];
            byte g = pointByes[25];
            byte b = pointByes[26];

            byte[] pointNewByes = new byte[15];
            byte[] xBytes = LittleEndianUtils.integerToBytes(newX);
            byte[] yBytes = LittleEndianUtils.integerToBytes(newY);
            byte[] zBytes = LittleEndianUtils.integerToBytes(newZ);

            for(int i=0;i<4;i++){
                pointNewByes[i] = xBytes[i];
            }
            for(int i=0;i<4;i++){
                pointNewByes[i+4] = yBytes[i];
            }
            for(int i=0;i<4;i++){
                pointNewByes[i+8] = zBytes[i];
            }
            pointNewByes[12] = r;
            pointNewByes[13] = g;
            pointNewByes[14] = b;

            return new Tuple2<String,byte[]>(nodeKey,pointByes);



        }).combineByKey((byte[] pointByes)->{
            List<byte[]> list = new ArrayList();
            list.add(pointByes);
            return list;
        },(List<byte[]> list1,byte[] pointByes)->{
            list1.add(pointByes);
            return list1;
        },(List<byte[]> list1,List<byte[]> list2)->{
            list1.addAll(list2);
            return list1;
        }).foreach((Tuple2<String,List<byte[]>> tuple2)->{
            String nodeKey = tuple2._1;
            Iterator<byte[]> iterator = tuple2._2.iterator();

            ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
            while (iterator.hasNext()){
                byte[]  pointBytes = iterator.next();
                byteOutputStream.write(pointBytes);
            }
            IOUtils.writerDataToFile(outputDirPath+File.separator+nodeKey+".bin",byteOutputStream.toByteArray(),true);
            byteOutputStream.close();
        });

    }

     */



    /**
     * 利用输出目录中所有的bin文件的文件名（即nodeKey），生成索引文件
     * @param binFilePathList 输出目录中的bin文件的文件地址
     * @param outputDirPath 输出目录
     */
    public static void createHrcFile(List<String> binFilePathList,String outputDirPath){

        List<String> nodeKeyList = binFilePathList.stream().map((binFilePath)->binFilePath.substring(binFilePath.lastIndexOf(File.separator)+1,binFilePath.lastIndexOf("."))).collect(Collectors.toList());

        byte[] hrcBytes = createHrcBytes(nodeKeyList);

        try {
            IOUtils.writerDataToFile(outputDirPath+File.separator+"r.hrc",hrcBytes,false);
        }catch (Exception e){
            e.printStackTrace();
        }

    }


    /**
     * 利用HBase数据库中的rowkey生成八叉树的索引文件
     *

    public static void createHrcRow(String tableName){
        Filter keyOnlyFilter = new KeyOnlyFilter();
        Map<String,byte[]> resultMap = HBaseUtils.scan(tableName,null,null,null,null,keyOnlyFilter);
        List<String> nodeKeyList = resultMap.keySet().stream().map(nodeKey -> nodeKey.split("-")[0].substring(1)).filter(nodeKey -> nodeKey.startsWith("r")).collect(Collectors.toList());
        byte[] hrcBytes = createHrcBytes(nodeKeyList);
        HBaseUtils.put(tableName,"hrc","data","hrc",hrcBytes);

    }
    */

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
