package com.pzx.geometry;

import com.pzx.IOUtils;
import com.pzx.distributedLock.DistributedRedisLock;
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

    public List<Tuple2<String, Integer>> shardToFile(double[] coordinatesScale, String outputDirPath){
        List<Tuple2<String, Integer>> nodeElementsTupleList = new ArrayList<>();
        rootLayer.traverse(new Grid3DLayer.Visitor() {
            @Override
            public boolean visit(Grid3DLayer grid3DLayer) {

                HashMap<String, List<byte[]>> buffer = new HashMap<>();
                Map<Long, List<Point3D>> cellElementsMap = grid3DLayer.getCellElementsMap();
                Cube totalBoundingBox = (Cube)grid3DLayer.getTotalRegion();

                for(Map.Entry<String, HashSet<Long>> entry : grid3DLayer.getNodeCellsMap().entrySet()){
                    String nodeKey = entry.getKey();
                    HashSet<Long> cellKeys = entry.getValue();

                    buffer.putIfAbsent(nodeKey, new ArrayList<>());
                    double[] xyzOffset = SplitUtils.getXYZOffset(nodeKey, totalBoundingBox);


                    for(Long cellKey : cellKeys){
                        List<byte[]> pointBytes = cellElementsMap.get(cellKey).stream().map(point3D -> point3D.serialize(xyzOffset, coordinatesScale)).collect(Collectors.toList());
                        buffer.get(nodeKey).addAll(pointBytes);
                    }

                }
                buffer.forEach((nodekey,list)->{
                    DistributedRedisLock.lock(nodekey);
                    String outputFilePath = outputDirPath+ File.separator+(nodekey.length()-1)+nodekey+".bin";
                    IOUtils.writerDataToFile(outputFilePath,list.iterator(),true);
                    DistributedRedisLock.unlock(nodekey);
                    nodeElementsTupleList.add(new Tuple2<String, Integer>(nodekey, list.size()));
                });
                return true;
            }
        });
        return nodeElementsTupleList;
    }

    public static void main(String[] args) {


        Grid3D grid3D = new Grid3D(new Cuboid(0,0,0,8,8,8),8.0/(1<<6),
                new Cube(0,0,0,8));

        long startTime = System.currentTimeMillis();

        int num = 0;
        for(int i =0 ; i< 3000000 ; i++){
            long time = System.currentTimeMillis();
            grid3D.insert(new Point3D(Math.random()* 3, Math.random()* 2 , Math.random()* 3));
            num++;
            long time1  = System.currentTimeMillis();
            if((time1 -time) > 10){
                logger.info("插入消耗时间："+ (time1 -time) + ",距离上一次慢插入期间插入了"+num+"个点");
                num = 0;
            }

        }



/*
        SparkSession sparkSession = SparkUtils.localSparkSessionInit();

        String inputDirPath = "D:\\wokspace\\点云的储存与可视化\\大数据集与工具\\data\\test.txt";

        //读取数据
        Dataset<Row> dataset = sparkSession.read()
                .format("csv")
                .option("sep", " ")
                .option("inferSchema", "true")
                .load(inputDirPath)
                .toDF("x", "y", "z", "intensity", "r", "g", "b")
                .selectExpr("x", "y", "z", "r", "g", "b");

        Dataset<Point3D> point3DDataset = dataset.map((MapFunction<Row, Point3D>) row -> {
            double x = row.getAs("x");
            double y = row.getAs("y");
            double z = row.getAs("z");
            int r = row.getAs("r");
            int g = row.getAs("g");
            int b = row.getAs("b");
            return new Point3D(x, y, z, (byte) r, (byte) g, (byte) b);
        }, Encoders.kryo(Point3D.class));

        long startTime = System.currentTimeMillis();

        List<Point3D> point3DList = point3DDataset.collectAsList();
        Grid3D grid3D = new Grid3D(new Cuboid(-26.861, -89.455, -1.887, 62.594, 0.0, 87.568),
                89.455 / (1 << 5),new Cuboid(-26.861, -89.455, -1.887, 62.594, 0.0, 87.568));

        int num = 0;
        for (int i = 0; i < point3DList.size(); i++) {
            long time = System.currentTimeMillis();
            Point3D point3D = point3DList.get(i);
            grid3D.insert(point3D);
            num++;

            long time1 = System.currentTimeMillis();
            if ((time1 - time) > 10) {
                logger.info("插入消耗时间：" + (time1 - time) + ",距离上一次慢插入期间插入了" + num + "个点");
                num = 0;
            }

        }

 */

            System.out.println("插入总耗时：" + (System.currentTimeMillis() - startTime));
            grid3D.printGrid3D();
            //System.out.println("总共插入点:" + point3DList.size());
            System.out.println("总共插入点:" + grid3D.getTotalElementsNum());
            //System.out.println(grid3DLayer.getLeafGridLevel());
            grid3D.shardToFile(new double[]{0.001, 0.001, 0.001}, "D:\\wokspace\\点云的储存与可视化\\大数据集与工具\\data\\新建文件夹");
            //System.out.println(grid3DLayer.getCellSideLength());
            //System.out.println(grid3DLayer.getCellRegion("1-1-1"));
        System.out.println("总耗时：" + (System.currentTimeMillis() - startTime));

    }


}
