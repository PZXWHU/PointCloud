package com.pzx.split;

import com.alibaba.fastjson.JSONObject;
import com.pzx.IOUtils;
import com.pzx.pointcloud.PointAttribute;
import com.pzx.pointcloud.PointCloud;
import com.pzx.utils.SplitUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @deprecated
 *
 */

public class TxtSplit
{

    public static int SPARK_DEFAULT_PARALLELISM = 30;

    public static Logger logger = Logger.getLogger(TxtSplit.class);

    public static void main ( String[] args ) throws IOException
    {

        //SparkSession spark = SparkSession.builder().appName("spilt pointCloud").master("spark://master:7077").getOrCreate();
        //spark.conf().set();
        //JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("spilt pointCloud").setMaster("spark://master:7077");
        sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        sparkConf.registerKryoClasses(new Class[]{String[].class,String.class,List.class});
        sparkConf.set("spark.executor.extraJavaOptions","-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/tmp/gc.log");//输出GC日志
        sparkConf.set("spark.driver.extraJavaOptions","-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/tmp/gc.log");
        sparkConf.set("spark.default.parallelism",SPARK_DEFAULT_PARALLELISM+"");
        sparkConf.set("spark.executor.memory","4g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);




        PointCloud pointCloud = new PointCloud();
        //pointCloud基本初始化
        pointCloud.setDimension(3);
        pointCloud.setScale(0.01);
        pointCloud.setPointNumPerNode(5000l);//设置每个节点含有5000个点左右
        List<PointAttribute> pointAttributes = new ArrayList<>();
        pointAttributes.add(PointAttribute.POSITION_CARTESIAN);
        pointCloud.setPointAttributes(pointAttributes);


        //filePath: 本地地址和hdfs地址均可，使用textFile读取
        //resultFilePath  本地地址和hdfs地址均可：hdfs://master:9000/pzx...

        //输入文本路径
        String filePath = Thread.currentThread().getContextClassLoader().getResource("")+"/txtdata";
        //生成结果路径
        String resultFilePath = "D:\\wokspace\\sparkdata";
        if(args.length==2){

            filePath = args[0];
            resultFilePath = args[1];
        }




        //读取文件，并将行文本分裂
        JavaRDD<String> fileRDD = sc.textFile(filePath,SPARK_DEFAULT_PARALLELISM);
        JavaRDD<String[]> pointAttributeRDD = fileRDD.map(x->x.split(","));
        pointAttributeRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());

        //遍历所有记录获得点云的包围盒，总点数，生成cloud.js文件
        long currentTime = System.currentTimeMillis();
        System.out.println("开始执行：");
        createCloudJS(pointCloud,pointAttributeRDD,resultFilePath);
        System.out.println("生成clodjs耗时："+(System.currentTimeMillis()-currentTime));


        //点云分片。生成数据文件
        JavaPairRDD<String,List<Object[]>> nodeKeyRDD = createnRDDGroupByNodeKey(pointCloud,pointAttributeRDD);
        nodeKeyRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
        createBinNodeFile(nodeKeyRDD,resultFilePath);
        System.out.println("生成clodjs耗时和生成数据文件："+(System.currentTimeMillis()-currentTime));


        createHrcFile(nodeKeyRDD,resultFilePath);
        System.out.println("生成clodjs耗时和生成数据文件和hrc："+(System.currentTimeMillis()-currentTime));




    }

    /**
     * 遍历所有记录获得点云的包围盒(TightBoundingBox,BoundingBox)，总点数，生成cloud.js文件
     * @param pointCloud
     * @param pointAttributeRDD
     */
    public static void createCloudJS(PointCloud pointCloud,JavaRDD<String[]> pointAttributeRDD,String resultFilePath){

        //获取点云的体积（包围盒），数量
        //返回数组[maxx,maxy,maxz,minx,miny,minz,pointNum]
        JavaRDD<Object[]> pointCloudAttributeRDD = pointAttributeRDD.mapPartitions(iterator -> {

            double maxx, maxy, maxz,minx,miny,minz;
            long pointNum = 0L;
            maxx = maxy = maxz = Double.MIN_VALUE;
            minx = miny = minz = Double.MAX_VALUE;

            while (iterator.hasNext()){
                String[] pointAttribute = iterator.next();
                maxx = Math.max(maxx,Double.parseDouble(pointAttribute[0]));
                maxy = Math.max(maxy,Double.parseDouble(pointAttribute[1]));
                maxz = Math.max(maxz,Double.parseDouble(pointAttribute[2]));
                minx = Math.min(minx,Double.parseDouble(pointAttribute[0]));
                miny = Math.min(miny,Double.parseDouble(pointAttribute[1]));
                minz = Math.min(minz,Double.parseDouble(pointAttribute[2]));
                pointNum++;
            }

            List result = new ArrayList<Object[]>();
            result.add(new Object[]{maxx,maxy,maxz,minx,miny,minz,pointNum});

            return result.iterator();
        });

        List<Object[]> pointCloudPartitionAttributeList =  pointCloudAttributeRDD.collect();
        double maxx, maxy, maxz,minx,miny,minz;
        long pointNum = 0L;
        maxx = maxy = maxz = Double.MIN_VALUE;
        minx = miny = minz = Double.MAX_VALUE;
        for(Object[] partitionAttribute : pointCloudPartitionAttributeList){
            maxx = Math.max(maxx,(double) partitionAttribute[0]);
            maxy = Math.max(maxy,(double) partitionAttribute[1]);
            maxz = Math.max(maxz,(double) partitionAttribute[2]);
            minx = Math.min(minx,(double) partitionAttribute[3]);
            miny = Math.min(miny,(double) partitionAttribute[4]);
            minz = Math.min(minz,(double) partitionAttribute[5]);
            pointNum = pointNum + (long)partitionAttribute[6];
        }

        double boundingBoxSideLength = Math.max(Math.max((maxx-minx),(maxy-miny)),(maxz-minz));
        //计算boundingBox
        double newMaxx = minx+boundingBoxSideLength;
        double newMaxy = miny+boundingBoxSideLength;
        double newMaxz = minz+boundingBoxSideLength;

        //设置pointCloud的一些属性
        pointCloud.setTightBoundingBox(new double[]{maxx, maxy, maxz,minx,miny,minz});
        pointCloud.setBoundingBox(new double[]{newMaxx, newMaxy, newMaxz,minx,miny,minz});
        pointCloud.setPointsNum(pointNum);


        JSONObject cloudJSObject = new JSONObject();
        cloudJSObject.put("version","1.7");
        cloudJSObject.put("octreeDir","data");
        cloudJSObject.put("projection","");
        cloudJSObject.put("points",pointNum);

        JSONObject boundingBoxJsonObject = new JSONObject();
        boundingBoxJsonObject.put("lx",minx);
        boundingBoxJsonObject.put("ly",miny);
        boundingBoxJsonObject.put("lz",minz);
        boundingBoxJsonObject.put("ux",newMaxx);
        boundingBoxJsonObject.put("uy",newMaxy);
        boundingBoxJsonObject.put("uz",newMaxz);
        cloudJSObject.put("boundingBox",boundingBoxJsonObject);

        JSONObject tightBoundingBoxJsonObject = new JSONObject();
        tightBoundingBoxJsonObject.put("lx",minx);
        tightBoundingBoxJsonObject.put("ly",miny);
        tightBoundingBoxJsonObject.put("lz",minz);
        tightBoundingBoxJsonObject.put("ux",maxx);
        tightBoundingBoxJsonObject.put("uy",maxy);
        tightBoundingBoxJsonObject.put("uz",maxz);
        cloudJSObject.put("tightBoundingBox",tightBoundingBoxJsonObject);

        List<PointAttribute> pointAttributes = pointCloud.getPointAttributes();


        List<String> pointAttributeStrArray = pointAttributes.stream().map( pointAttribute -> pointAttribute.name()).collect(Collectors.toList());

        cloudJSObject.put("pointAttributes",pointAttributeStrArray);

        double diagonal = Math.pow(Math.pow(newMaxx-minx,2)+Math.pow(newMaxy-miny,2)+Math.pow(newMaxz-minz,2),1.0/3);
        cloudJSObject.put("spacing",diagonal/250);

        cloudJSObject.put("scale",pointCloud.getScale());
        cloudJSObject.put("hierarchyStepSize",100);

        String jsonStr = cloudJSObject.toJSONString();

        try {
            IOUtils.writerDataToFile(resultFilePath+File.separator+"cloud.js",jsonStr.getBytes("utf-8"),false);
        }catch (Exception e){
            e.printStackTrace();

        }



    }

    /**
     *
     * @param pointCloud
     * @param pointAttributeRDD
     * @return RDD <nodekey,[point1AttributeArray,point2AttributeArray....]
     *  point1AttributeArray = [x,y,z]
     */
    public static JavaPairRDD<String,List<Object[]>> createnRDDGroupByNodeKey(PointCloud pointCloud,JavaRDD<String[]> pointAttributeRDD){
        //计算一些数值用于spark计算
        int maxLevel = SplitUtils.getMaxLevel(pointCloud.getPointsNum(),pointCloud.getPointNumPerNode(),pointCloud.getDimension());
        pointCloud.setMaxLevel(maxLevel);
        int dimension = pointCloud.getDimension();
        double[] boundingBox = pointCloud.getBoundingBox();
        double scale = pointCloud.getScale();

        JavaPairRDD<String,List<Object[]>> nodeKeyRDD  = pointAttributeRDD.mapToPair(pointAttribute->{

            //logger.debug("-----------------------");
            //logger.debug("pointAttribute.length:"+pointAttribute.length);

            double x = Double.parseDouble(pointAttribute[0]);
            double y = Double.parseDouble(pointAttribute[1]);
            double z = Double.parseDouble(pointAttribute[2]);
            double clod = SplitUtils.getClod(maxLevel,dimension);

            //logger.debug("boundingBox.length:"+boundingBox.length);
            String nodeKey = SplitUtils.getOctreeNodeName(x,y,z,boundingBox,clod);

            //double[] offsets = SplitUtils.getXYZOffset(nodeKey,boundingBox);
            int newX = (int)(x/scale);
            int newY = (int)(y/scale);
            int newZ = (int)(z/scale);

            //float newClod = (float)


            Object[] newPointAttribute = new Object[]{newX,newY,newZ};
            return new Tuple2<>(nodeKey,newPointAttribute);
        }).combineByKey((Object[] newPointAttribute) -> {
                    List<Object[]> list = new ArrayList();
                    list.add(newPointAttribute);
                    return list;
                },
                (List<Object[]> list, Object[] objects) -> {
                    list.add(objects);
                    return list;
                },
                (List<Object[]> list1, List<Object[]> list2) -> {
                    list1.addAll(list2);
                    return list1;
                }, new Partitioner() {
                    @Override
                    public int getPartition(Object key) {
                        return Math.abs(key.toString().hashCode()%numPartitions());
                    }

                    @Override
                    public int numPartitions() {
                        return SPARK_DEFAULT_PARALLELISM;
                    }
                });
        return nodeKeyRDD;
    }


    public static void createBinNodeFile(JavaPairRDD<String,List<Object[]>> nodeKeyRDD,String resultFilePath){

        nodeKeyRDD.foreach((Tuple2<String,List<Object[]>> tuple2)->{
            String nodeKey = tuple2._1;
            Iterator<Object[]> iterator = tuple2._2.iterator();

            ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
            while (iterator.hasNext()){

                Object[]  point = iterator.next();


                IOUtils.writeIntLittleEndian(byteOutputStream,(int)point[0]);
                IOUtils.writeIntLittleEndian(byteOutputStream,(int)point[1]);
                IOUtils.writeIntLittleEndian(byteOutputStream,(int)point[2]);
            }

            IOUtils.writerDataToFile(resultFilePath+File.separator+nodeKey+".bin",byteOutputStream.toByteArray(),false);
            byteOutputStream.close();
        });

    }

    public static void createHrcFile(JavaPairRDD<String,List<Object[]>> nodeKeyRDD,String resultFilePath){
        Map<String,Long> nodeMap = new HashMap<>();

        //获取《nodekey pointcount》 map
        nodeKeyRDD.map((Tuple2<String,List<Object[]>> tuple2)->{
            String nodeKey = tuple2._1;
            Iterator<Object[]> iterator = tuple2._2.iterator();
            long pointCount = 0L;
            while (iterator.hasNext()){
                iterator.next();
                pointCount++;
            }
            return new Tuple2<String,Long>(nodeKey,pointCount);
        }).collect().forEach((Tuple2<String,Long> tuple2)->{
            String nodeKey = tuple2._1;
            Long pointCount = tuple2._2;
            nodeMap.put(nodeKey,pointCount);
        });

        //对nodekey进行排序  符合hrc文件顺序要求
        TreeMap<String, Pair<Byte,Integer>> hrcMap = new TreeMap<>(new Comparator<String>() {
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

        //将遍历map里面的nodekey并检测map里是否含有其孩子，以此判断其mask
        for (Map.Entry<String,Long> entry : nodeMap.entrySet()) {
            String nodeKey = entry.getKey();
            int pointCount = entry.getValue().intValue();
            byte mask = 0;
            for(int i=0;i<8;i++){
                if(nodeMap.containsKey(nodeKey+i))
                    mask|=1<<i;
            }
            hrcMap.put(nodeKey,Pair.of(mask,pointCount));
        }

        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            for(Map.Entry<String,Pair<Byte,Integer>> entry = hrcMap.pollFirstEntry();entry!=null;entry=hrcMap.pollFirstEntry()){
                Pair<Byte,Integer> pair = entry.getValue();
                byteArrayOutputStream.write(pair.getKey());
                //IOUtils.writeIntLittleEndian(dataOutputStream,pair.getValue());
            }

            IOUtils.writerDataToFile(resultFilePath+ File.separator+"r.hrc",byteArrayOutputStream.toByteArray(),false);
            byteArrayOutputStream.close();

        }catch (java.lang.Exception e){
            e.printStackTrace();
        }


    }




}
