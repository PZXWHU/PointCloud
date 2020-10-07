package com.pzx.geometry;

import com.pzx.IOUtils;
import com.pzx.dataSplit.TxtSplit2;
import com.pzx.distributedLock.DistributedRedisLock;
import com.pzx.pointCloud.HrcFile;
import com.pzx.utils.SparkUtils;
import com.pzx.utils.SplitUtils;
import javafx.scene.chart.CategoryAxisBuilder;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

public class Grid3D {

    private final static Logger logger = Logger.getLogger(Grid3D.class);

    private Grid3DLayer rootLayer;

    public Grid3D(Grid3DLayer rootLayer) {
        this.rootLayer = rootLayer;
    }

    public Grid3D(Cuboid region, double cellSideLength, Cube totalRegion) {
        this(region, cellSideLength, totalRegion, 15);
    }

    public Grid3D(Cuboid region, double cellSideLength, Cube totalRegion, int maxLevel) {
        this.rootLayer = new Grid3DLayer(region, cellSideLength,0, totalRegion, maxLevel);
    }


    public void insert(Point3D point3D){
        rootLayer.insert(point3D);
    }

    public int getGridMaxLevel(){return rootLayer.getLeafGridLevel();}

    public void printGrid3D(){
        rootLayer.traverse(new Grid3DLayer.Visitor() {
            @Override
            public boolean visit(Grid3DLayer grid3DLayer) {
                System.out.println("当前网格level："+ grid3DLayer.getGridLevel() + ",是否为叶节点："+ grid3DLayer.isLeaf());
                System.out.println("网格含有点数量：" + grid3DLayer.getElementsNum());
                System.out.println("网格含有网格单元数量：" + grid3DLayer.getMaxCellNum());
                System.out.println("含有点数是否小于网格数：" + (grid3DLayer.getElementsNum() < grid3DLayer.getCellElementsMap().size()));
                System.out.println(grid3DLayer.getNodeMaxCellNumMap());
                System.out.println("网格的含有的最大单元格数量："+ grid3DLayer.getNodeMaxCellNumMap().values().stream().mapToInt(i -> i.intValue()).sum());
                System.out.println(grid3DLayer.getNodeElementNumMap());
                System.out.println("网格的含有的点数量："+ grid3DLayer.getNodeElementNumMap().values().stream().mapToInt(i -> i.intValue()).sum());
                /*
                System.out.println("网格单元长度：" + grid3DLayer.getCellSideLength());
                System.out.println("网格单元最大数：" + grid3DLayer.getCellTotalNum());
                System.out.println("网格范围：" + grid3DLayer.getGridRegion());
                for(Map.Entry<Long, List<Point3D>> entry : grid3DLayer.getGridCells().entrySet()){
                    System.out.println("cell边界： "+ grid3DLayer.getCellRegion(entry.getKey())+
                            ", cell含有点数量："+ entry.getValue().size() +
                            ", 含有点："+entry.getValue().get(0));
                }*/
                System.out.println("--------------------------------");
                return true;
            }


        });
    }

    public long getTotalElementsNum(){
        MutableLong totalElementsNum = new MutableLong(0);
        rootLayer.traverse(new Grid3DLayer.Visitor() {
            @Override
            public boolean visit(Grid3DLayer grid3DLayer) {
                totalElementsNum.add(grid3DLayer.getElementsNum());
                return true;
            }
        });
        return totalElementsNum.getValue();
    }

    public HashMap<String, List<byte[]>> shardToNode(double[] coordinatesScale){
        HashMap<String, List<byte[]>> nodeElementsBuffer = new HashMap<>();
        rootLayer.traverse(new Grid3DLayer.Visitor() {
            @Override
            public boolean visit(Grid3DLayer grid3DLayer) {

                Map<Long, List<Point3D>> cellElementsMap = grid3DLayer.getCellElementsMap();
                Cube totalBoundingBox = (Cube)grid3DLayer.getTotalRegion();

                for(Map.Entry<String, HashSet<Long>> entry : grid3DLayer.getNodeCellsMap().entrySet()){
                    String nodeKey = entry.getKey();
                    HashSet<Long> cellKeys = entry.getValue();

                    nodeElementsBuffer.putIfAbsent(nodeKey, new ArrayList<>());
                    double[] xyzOffset = SplitUtils.getXYZOffset(nodeKey, totalBoundingBox);

                    for(Long cellKey : cellKeys){
                        List<byte[]> pointBytes = cellElementsMap.get(cellKey).stream().map(point3D -> point3D.serialize(xyzOffset, coordinatesScale)).collect(Collectors.toList());
                        nodeElementsBuffer.get(nodeKey).addAll(pointBytes);
                    }
                }
                return true;
            }
        });
        return nodeElementsBuffer;
    }

    public List<Tuple2<String, Integer>> shardToFile(double[] coordinatesScale, String outputDirPath){
        List<Tuple2<String, Integer>> nodeElementsTupleList = new ArrayList<>();
        HashMap<String, List<byte[]>> nodeElementsBuffer = shardToNode(coordinatesScale);
        nodeElementsBuffer.forEach((nodeKey,list)->{
            DistributedRedisLock.lock(nodeKey);
            String outputFilePath = outputDirPath + File.separator + SplitUtils.createBinFileName(nodeKey);
            IOUtils.writerDataToFile(outputFilePath,list.iterator(),true);
            DistributedRedisLock.unlock(nodeKey);
            nodeElementsTupleList.add(new Tuple2<String, Integer>(nodeKey, list.size()));
        });
        return nodeElementsTupleList;
    }

}
