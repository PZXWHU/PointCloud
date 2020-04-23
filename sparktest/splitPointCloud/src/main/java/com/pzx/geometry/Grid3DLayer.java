package com.pzx.geometry;

import com.google.common.base.Preconditions;
import com.pzx.IOUtils;

import com.pzx.utils.SplitUtils;
import org.apache.log4j.Logger;


import java.io.File;
import java.util.*;
import java.util.stream.Collectors;


public class Grid3DLayer {

    private final static Logger logger = Logger.getLogger(Grid3DLayer.class);
    private String cellKeySeparator = "-";

    private int maxCellNum;
    private int cellNumXSide;
    private int cellNumYSide;
    private int cellNumZSide;
    private int level;
    private Grid3DLayer child;
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


    public Grid3DLayer(Cuboid region, double cellSideLength){
        this(region, cellSideLength ,0);
    }

    public Grid3DLayer(Cuboid region, double cellSideLength , int level){
        this.region = region;
        this.cellSideLength = cellSideLength;
        this.cellNumXSide = (int)Math.ceil(region.getXSideLength() / cellSideLength);
        this.cellNumYSide = (int)Math.ceil(region.getYSideLength() / cellSideLength);
        this.cellNumZSide = (int)Math.ceil(region.getZSideLength() / cellSideLength);
        this.maxCellNum = cellNumXSide * cellNumYSide * cellNumZSide;

        this.level = level;
        this.elementsNum = 0;
        this.child = null;
    }

    public void insert(Point3D pointToInsert){
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
            child.insert(pointToInsert);
            return;
        }

        //替换cell中的点
        cellElementsList.set(0, pointToInsert);
        child.insert(pointInCell);

    }

    /**
     * 如果网格中点数量超过网格单元数量，则将网格中的点向下渗透
     */
    public void maySplit(){

        //当元素数大于网格的1/3时就开始分裂
        if(elementsNum > maxCellNum*(2.0/5)){
            //long time  = System.currentTimeMillis();
            child = createChildGrid3DLayer();

            for (Map.Entry<Long, List<Point3D>> entry : gridCells.entrySet()){

                Long cellKey = entry.getKey();
                List<Point3D> cellElements = entry.getValue();

                Point3D cellCenter = getCellCenter(cellKey);
                Point3D stayedPoint = cellElements.get(0);

                for(int i= 1; i<cellElements.size() ; i++){
                    Point3D pointInCell = cellElements.get(i);
                    if(cellCenter.distance(stayedPoint) < cellCenter.distance(pointInCell)){
                        child.insert(pointInCell);
                        continue;
                    }
                    //替换需要保留的点
                    child.insert(stayedPoint);
                    stayedPoint = pointInCell;
                }

                cellElements.clear();
                cellElements.add(stayedPoint);

            }

            elementsNum = gridCells.size();

            //logger.info("分裂耗时："+ (System.currentTimeMillis() - time));
        }
    }


    private Grid3DLayer createChildGrid3DLayer(){
        return new Grid3DLayer(region, cellSideLength / 2.0 , level + 1);
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

    public Point3D getCellCenter(long gridCellKey){
        return getCellRegion(gridCellKey).centerPoint();
    }

    public Cube getCellRegion(long gridCellKey){

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

    public int getMaxCellNum() {
        return maxCellNum;
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

    public static interface Visitor{
        /**
         * Visits a single node of the tree
         *
         * @param grid3DLayer grid to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(Grid3DLayer grid3DLayer);
    }

    public void traverse(Visitor visitor){
        if(!visitor.visit(this))
            return;
        if (child != null)
            child.traverse(visitor);
    }

}
