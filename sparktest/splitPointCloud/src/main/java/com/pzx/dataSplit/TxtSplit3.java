package com.pzx.dataSplit;

import com.google.common.base.Preconditions;
import com.pzx.IOUtils;
import com.pzx.geometry.Cube;
import com.pzx.geometry.Cuboid;
import com.pzx.geometry.Point3D;
import com.pzx.index.ocTree.OcTree;
import com.pzx.pointCloud.HrcFile;
import com.pzx.pointCloud.PointCloud;
import com.pzx.spatialPartition.CustomPartitioner;
import com.pzx.spatialPartition.OcTreePartitioner;
import com.pzx.spatialPartition.OcTreePartitioning;
import com.pzx.utils.NormalVectorUtils;
import com.pzx.utils.SparkUtils;
import org.apache.log4j.net.SocketServer;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.PartitionPruningRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.In;
import org.mortbay.jetty.HttpConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;
import scala.runtime.AbstractFunction1;

import javax.naming.event.NamingListener;
import java.io.*;
import java.net.ServerSocket;
import java.nio.Buffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 八叉树分区 + 计算法向量（空间索引）
 * + 利用网格构造LOD和分片
 */
public class TxtSplit3 extends ComputeNormalVector  {

    private final static Logger logger = LoggerFactory.getLogger(TxtSplit3.class);

    public TxtSplit3(String inputDirPath, String outputDirPath) {
        super(inputDirPath, outputDirPath);

    }


    @Override
    protected void splitData() {

        //空间分区
        JavaPairRDD<Integer, Point3D> partitionedRDDWithPartitionID = spatialPartitioning();
        //分区裁剪
        JavaPairRDD<Integer,Point3D> prunedRDDWithOriginalPartitionID = partitionsPruning(partitionedRDDWithPartitionID);

        JavaRDD<Point3D> pointsRDDWithOriginalPartitionID = computeNormalVector(prunedRDDWithOriginalPartitionID);

        //pointsRDDWithOriginalPartitionID.foreach(tuple->{});
        writePointsToLocalFile(pointsRDDWithOriginalPartitionID, "D:\\wokspace\\点云数据集\\大数据集与工具\\data\\新建文件夹\\spatialIndex\\");

        //再次进行分区裁剪，因为有些分区没有点，但是因为空间分区时是利用点的缓冲区判断的，可能发生数据冗余，导致某些分区只含有其他分区中的点
        //如果需要输出，则需要此行代码
        //prunedRDDWithOriginalPartitionID = partitionsPruning(pointsRDDWithOriginalPartitionID);
        /*
        nodeElementsRDD = shardToNode(prunedRDDWithOriginalPartitionID);
        writeNode();
         */

    }



    /**
     * 利用八叉树对RDD进行分区

     * @return
     */
    protected JavaPairRDD<Integer, Point3D> spatialPartitioning(){

        partitioner = getPartitioner(rowDataSet, sampleFraction);

        JavaRDD<Point3D> pointJavaRDD = rowDataSet.map((MapFunction<Row, Point3D>) row->rowToPoint3D(row), Encoders.kryo(Point3D.class)).toJavaRDD();

        //将knn转换为范围查询，根据平均密度决定范围查询的长度
        double pointDensity = pointCloud.points / pointCloud.tightBoundingBox.getVolume();
        searchCuboidLength = Math.pow(k / pointDensity, 1 / 3.0);


        JavaPairRDD<Integer,Point3D> partitionedRDDWithPartitionID = pointJavaRDD.mapPartitionsToPair(pointIterator -> {

            List<Tuple2<Integer, Point3D>> result = new ArrayList<>();
            List<Cuboid> partitionRegions = partitioner.getPartitionRegions();

            while (pointIterator.hasNext()){
                Point3D point3D = pointIterator.next();
                Cuboid searchCuboid = Cuboid.createFromCenterPointAndSideLength(point3D, searchCuboidLength, searchCuboidLength, searchCuboidLength);
                //以搜索半径作为缓冲区半径（长方体缓冲区），将点冗余到多个分区中，并且注明点的notLocal的属性
                List<Integer> partitionIDs = partitioner.findPartitionIDs(searchCuboid);
                for(Integer partitionID : partitionIDs){
                    if (!partitionRegions.get(partitionID).contains(point3D)){
                        Point3D outOfPartitionRegionPoint = point3D.copy();
                        outOfPartitionRegionPoint.setIsLocalPartition(false);
                        result.add(new Tuple2<Integer, Point3D>(partitionID, outOfPartitionRegionPoint));//数据冗余
                    }else
                        result.add(new Tuple2<Integer, Point3D>(partitionID, point3D));//数据冗余
                }
            }
            return result.iterator();
        }).partitionBy(partitioner);

        return partitionedRDDWithPartitionID;
    }

    protected JavaRDD<Point3D> computeNormalVector(JavaPairRDD<Integer, Point3D> pointRDDWithOriginalPartitionID) {

        JavaPairRDD<Integer, OcTree<Point3D>> localIndexRDDWithOriginalPartitionID = createLocalIndex(pointRDDWithOriginalPartitionID);

        JavaRDD<Point3D> pointsRDDWithOriginalPartitionID = localIndexRDDWithOriginalPartitionID.mapPartitions(iterator ->{
            //一个分区上建立了一颗八叉树
            Tuple2<Integer, OcTree<Point3D>> tuple = iterator.next();
            OcTree<Point3D> ocTree = tuple._2;
            Set<Point3D> points = ocTree.getAllElements();
            Cuboid searchCuboid = Cuboid.createFromMinAndMaxCoordinate(0,0,0,0,0,0);

            Set<Point3D> resultSet = new HashSet<>();
            for(Point3D point : points){

                if (point.isLocalPartition()){
                    //long t = System.currentTimeMillis();
                    searchCuboid.setMinX(point.x - searchCuboidLength / 2);
                    searchCuboid.setMinY(point.y - searchCuboidLength / 2);
                    searchCuboid.setMinZ(point.z - searchCuboidLength / 2);
                    searchCuboid.setMaxX(point.x + searchCuboidLength / 2);
                    searchCuboid.setMaxY(point.y + searchCuboidLength / 2);
                    searchCuboid.setMaxZ(point.z + searchCuboidLength / 2);
                    List<Point3D> neighborPoints = ocTree.rangeQuery(searchCuboid);
                    point.setNormalVector(NormalVectorUtils.getNormalVector(neighborPoints, k));
                    resultSet.add(point);
                    //logger.info("此次查询需要时间： " + (System.currentTimeMillis() - t));
                }
            }
            return resultSet.iterator();

        });
        return pointsRDDWithOriginalPartitionID;
    }

    private JavaPairRDD<Integer, OcTree<Point3D>> createLocalIndex(JavaPairRDD<Integer,Point3D> prunedRDDWithOriginalPartitionID){
        List<Cuboid> partitionRegions = partitioner.getPartitionRegions();
        //广播变量
        Broadcast<List<Cuboid>> partitionRegionsBroadcast = sparkSession.sparkContext().broadcast(partitionRegions, ClassManifestFactory.classType(List.class));

        JavaPairRDD<Integer, OcTree<Point3D>> localIndexRDDWithOriginalPartitionID = prunedRDDWithOriginalPartitionID.mapPartitionsToPair((iterator) ->{

            Tuple2<Integer, Point3D> pointWithOriginalPartitionID = iterator.next();
            int originalPartitionID = pointWithOriginalPartitionID._1;
            Cuboid partitionRegion = partitionRegionsBroadcast.getValue().get(originalPartitionID);
            OcTree<Point3D> ocTree = new OcTree<>(partitionRegion.expand(searchCuboidLength), k, 10);
            ocTree.insert(pointWithOriginalPartitionID._2);
            while (iterator.hasNext()){
                ocTree.insert(iterator.next()._2);
            }
            return Collections.singletonList(new Tuple2<Integer, OcTree<Point3D>>(originalPartitionID, ocTree)).iterator();
        });

        return localIndexRDDWithOriginalPartitionID;

    }

    @Override
    protected void sparkSessionInit() {
        sparkSession = SparkUtils.localSparkSessionInit();
    }

    public static void main(String[] args) {
        Preconditions.checkArgument(args.length==2,"inputDirPath and outputDirPath is needed！");

        //输入本地输入路径
        String inputDirPath = args[0];
        //生成结果输出路径
        String outputDirPath = args[1];

        DataSplitter splitter = new TxtSplit3(inputDirPath, outputDirPath);
        splitter.dataSplit();
    }

}
