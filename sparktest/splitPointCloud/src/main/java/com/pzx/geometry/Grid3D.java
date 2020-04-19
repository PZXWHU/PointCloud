package com.pzx.geometry;

import com.google.common.base.Preconditions;
import com.pzx.IOUtils;

import com.pzx.distributedLock.DistributedRedisLock;
import com.pzx.utils.SplitUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.log4j.Logger;


import java.io.File;
import java.util.*;
import java.util.stream.Collectors;


public class Grid3D {

    private final static Logger logger = Logger.getLogger(Grid3D.class);
    private String cellKeySeparator = "-";

    private int cellTotalNum;
    private int cellNumXSide;
    private int cellNumYSide;
    private int cellNumZSide;
    private int level;
    private Grid3D child;
    private Cuboid region;
    private double cellSideLength;
    private long elementsNum;

    //用Map储存网格单元及其包含点
    private Map<Long, List<Point3D>> gridCells = new HashMap<>();

    //private Map<Long, Queue<Point3D>> gridCells = new HashMap<>();

    public Comparator<Point3D> closerToCellCenterComparator(Point3D cellCenter){
        return new Comparator<Point3D>() {
            @Override
            public int compare(Point3D o1, Point3D o2) {
                if(cellCenter.distance(o1) < cellCenter.distance(o2))
                    return 1;
                else if (cellCenter.distance(o1) == cellCenter.distance(o2))
                    return 0;
                else
                    return -1;
            }
        };
    }


    public Grid3D(Cuboid region, double cellSideLength){
        this(region, cellSideLength ,0);
    }

    public Grid3D(Cuboid region, double cellSideLength ,int level){
        this.region = region;
        this.cellSideLength = cellSideLength;
        this.cellNumXSide = (int)Math.ceil(region.getXSideLength() / cellSideLength);
        this.cellNumYSide = (int)Math.ceil(region.getYSideLength() / cellSideLength);
        this.cellNumZSide = (int)Math.ceil(region.getZSideLength() / cellSideLength);
        this.cellTotalNum = cellNumXSide * cellNumYSide * cellNumZSide;
        this.level = level;
        this.elementsNum = 0;
        this.child = null;
    }

    public void inset(Point3D pointToInsert){
        Preconditions.checkArgument(region.contains(pointToInsert), "the insert point is not in the grid region！");

        Long gridCellKey = getGridCellKey(pointToInsert);

        gridCells.putIfAbsent(gridCellKey, new ArrayList<Point3D>());
        List<Point3D> cellElementsList = gridCells.get(gridCellKey);

        //如果是当前网格是叶子节点或者网格单元中没有点，直接插入
        if(isLeaf() || cellElementsList.size() ==0){
            cellElementsList.add(pointToInsert);
            elementsNum++;

            //叶子节点需要考虑分裂
            if(isLeaf())
                maySplit();
            return;
        }

        Point3D pointInCell = cellElementsList.get(0);
        Point3D cellCenter = getCellCenter(gridCellKey);

        if(cellCenter.distance(pointToInsert) > cellCenter.distance(pointInCell)){
            child.inset(pointToInsert);
            return;
        }

        //替换cell中的点
        cellElementsList.set(0, pointToInsert);
        child.inset(pointInCell);

        /*
        Point3D cellCenter = getCellCenter(gridCellKey);
        Comparator<Point3D> closerToCellCenterComparator = closerToCellCenterComparator(cellCenter);

        gridCells.putIfAbsent(gridCellKey, new PriorityQueue<>(closerToCellCenterComparator));
        Queue<Point3D> cellElementsQueue = gridCells.get(gridCellKey);

        //如果是当前网格是叶子节点或者网格单元中没有点，直接插入
        if(isLeaf() || cellElementsQueue.size() ==0){
            cellElementsQueue.add(pointToInsert);
            elementsNum++;

            //叶子节点需要考虑分裂
            if(isLeaf())
                maySplit();
            return;
        }

        if(closerToCellCenterComparator.compare(cellElementsQueue.peek(), pointToInsert)>=0){
            child.inset(pointToInsert);
            return;
        }

        child.inset(cellElementsQueue.poll());
        cellElementsQueue.add(pointToInsert);
        */

    }

    /**
     * 如果网格中点数量超过网格单元数量，则将网格中的点向下渗透
     */
    public void maySplit(){

        //当元素数大于网格的1/3时就开始分裂
        if(elementsNum > cellTotalNum*(1.0/2)){
            //long time  = System.currentTimeMillis();
            child = createChildGrid3D();

            for (Map.Entry<Long, List<Point3D>> entry : gridCells.entrySet()){

                Long cellKey = entry.getKey();
                List<Point3D> cellElements = entry.getValue();

                Point3D cellCenter = getCellCenter(cellKey);
                Point3D stayedPoint = cellElements.get(0);

                for(int i= 1; i<cellElements.size() ; i++){
                    Point3D pointInCell = cellElements.get(i);
                    if(cellCenter.distance(stayedPoint) < cellCenter.distance(pointInCell)){
                        child.inset(pointInCell);
                        continue;
                    }
                    //替换需要保留的点
                    child.inset(stayedPoint);
                    stayedPoint = pointInCell;
                }

                cellElements.clear();
                cellElements.add(stayedPoint);

            }

            /*
            for (Map.Entry<Long, Queue<Point3D>> entry : gridCells.entrySet()){

                Long cellKey = entry.getKey();
                Queue<Point3D> cellElementsQueue = entry.getValue();


                Point3D stayedPoint = cellElementsQueue.poll();

                Iterator<Point3D> pointInertIntoChildren = cellElementsQueue.iterator();
                while (pointInertIntoChildren.hasNext()){
                    child.inset(pointInertIntoChildren.next());
                }
                cellElementsQueue.clear();
                cellElementsQueue.add(stayedPoint);
            }

            elementsNum = gridCells.size();
             */
            //logger.info("分裂耗时："+ (System.currentTimeMillis() - time));
        }
    }

    public void shardToFile(double[] boundingBox,  double[] coordinatesScale, String outputDirPath){
        this.traverse(new Visitor() {
            @Override
            public boolean visit(Grid3D grid3D) {
                HashMap<String, List<byte[]>> buffer = new HashMap<>();

                for(Map.Entry<Long, List<Point3D> > entry : grid3D.getGridCells().entrySet()){

                    //一个网格单元一定属于同一各分片
                    Point3D cellCenter = getCellCenter(entry.getKey());
                    String nodeKey = SplitUtils.getOctreeNodeName(cellCenter, boundingBox, grid3D.getGridLevel());

                    List<Point3D> cellElements = entry.getValue();

                    buffer.putIfAbsent(nodeKey, new ArrayList<>());
                    double[] xyzOffset = SplitUtils.getXYZOffset(nodeKey, boundingBox);
                    List<byte[]> pointBytesList = cellElements.stream().map(point3D -> point3D.serialize(xyzOffset, coordinatesScale)).collect(Collectors.toList());
                    buffer.get(nodeKey).addAll(pointBytesList);
                }

                buffer.forEach((nodekey,list)->{
                    //DistributedRedisLock.lock(nodekey);
                    String outputFilePath = outputDirPath+ File.separator+(nodekey.length()-1)+nodekey+".bin";
                    IOUtils.writerDataToFile(outputFilePath,list.iterator(),true);
                    //DistributedRedisLock.unlock(nodekey);
                });

                return true;
            }
        });
    }

    public static void main(String[] args) {
        Grid3D grid3D = new Grid3D(new Cuboid(0,0,0,8,8,8),2);

        long startTime = System.currentTimeMillis();

        int num = 0;
        for(int i =0 ; i< 10000000 ; i++){
            long time = System.currentTimeMillis();
            grid3D.inset(new Point3D(Math.random()* 8, Math.random()* 8 , Math.random()* 8));
            num++;
            long time1  = System.currentTimeMillis();
            if((time1 -time) > 10){
                logger.info("插入消耗时间："+ (time1 -time) + ",距离上一次慢插入期间插入了"+num+"个点");
                num = 0;
            }

        }

        System.out.println("插入总耗时："+ (System.currentTimeMillis() -startTime) );
        //grid3D.printGrid3D();
        //System.out.println(grid3D.getLeafGridLevel());
        //grid3D.shardToFile(new double[]{8,8,8,2,2,2},new double[]{ 0.001,0.001,0.001},"D:\\wokspace\\点云的储存与可视化\\大数据集与工具\\data\\新建文件夹");
        //System.out.println(grid3D.getCellSideLength());
        //System.out.println(grid3D.getCellRegion("1-1-1"));

    }

    public void printGrid3D(){
        traverse(new Visitor() {
            @Override
            public boolean visit(Grid3D grid3D) {
                System.out.println("当前网格level："+ grid3D.getGridLevel() + ",是否为叶节点："+ grid3D.isLeaf());
                System.out.println("网格含有点数量：" + grid3D.getElementsNum());
                System.out.println("含有点数是否小于网格数：" + (grid3D.getElementsNum() < grid3D.getCellTotalNum()));
                /*
                System.out.println("网格单元长度：" + grid3D.getCellSideLength());
                System.out.println("网格单元最大数：" + grid3D.getCellTotalNum());
                System.out.println("网格范围：" + grid3D.getGridRegion());
                for(Map.Entry<Long, List<Point3D>> entry : grid3D.getGridCells().entrySet()){
                    System.out.println("cell边界： "+ grid3D.getCellRegion(entry.getKey())+
                            ", cell含有点数量："+ entry.getValue().size() +
                            ", 含有点："+entry.getValue().get(0));
                }*/
                System.out.println("--------------------------------");
                return true;
            }


        });
    }

    private Grid3D createChildGrid3D(){
        return new Grid3D(region, cellSideLength / 2 , level + 1);
    }

    /**
     * 根据点在x，y，z轴的位置，获取点所属的网格单元的key
     * @param point3D
     * @return
     */
    private long getGridCellKey(Point3D point3D){
        long xSideIndex = (long)Math.floor((point3D.x - region.getMinX()) / cellSideLength);
        long ySideIndex = (long)Math.floor((point3D.y - region.getMinY()) / cellSideLength);
        long zSideIndex = (long)Math.floor((point3D.z - region.getMinZ()) / cellSideLength);

        //以long型的后六十位储存
        long cellKey = xSideIndex<<40 | ySideIndex<<20 | zSideIndex;

        return cellKey;
    }

    private Point3D getCellCenter(long gridCellKey){
        return getCellRegion(gridCellKey).centerPoint();
    }

    private Cube getCellRegion(long gridCellKey){

        long xSideIndex = gridCellKey >>> 40 & 0xfffffL;
        long ySideIndex = gridCellKey >>> 20 & 0xfffffL;
        long zSideIndex = gridCellKey & 0xfffffL;

        double cellMinX = region.getMinX() + xSideIndex * cellSideLength;
        double cellMinY = region.getMinY() + ySideIndex * cellSideLength;
        double cellMinZ = region.getMinZ() + zSideIndex * cellSideLength;

        return new Cube(cellMinX, cellMinY, cellMinZ, cellSideLength);
    }

    public int getLeafGridLevel(){
        if(child == null)
            return this.level;
        return child.getLeafGridLevel();
    }

    public boolean isLeaf(){
        return child == null;
    }

    public int getCellTotalNum() {
        return cellTotalNum;
    }

    public int getGridLevel() {
        return level;
    }

    public Cuboid getGridRegion() {
        return region;
    }

    public long getElementsNum() {
        return elementsNum;
    }

    public double getCellSideLength() {
        return cellSideLength;
    }

    public Map<Long, List<Point3D>> getGridCells() {
        return gridCells;
    }

    private static interface Visitor{
        /**
         * Visits a single node of the tree
         *
         * @param grid3D grid to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(Grid3D grid3D);
    }

    public void traverse(Visitor visitor){
        if(!visitor.visit(this))
            return;
        if (child != null)
            child.traverse(visitor);
    }

}
