package com.pzx.geometry;

import com.google.common.base.Preconditions;
import com.pzx.IOUtils;

import com.pzx.utils.SplitUtils;
import com.sun.corba.se.impl.oa.toa.TOA;
import org.apache.log4j.Logger;
import static com.pzx.pointCloud.PointCloud.*;


import javax.print.DocFlavor;
import java.io.File;
import java.util.*;
import java.util.stream.Collectors;


public class Grid3DLayer {

    private final static Logger logger = Logger.getLogger(Grid3DLayer.class);

    private long maxCellNum;
    private long cellNumXSide;
    private long cellNumYSide;
    private long cellNumZSide;
    private int level;
    private Grid3DLayer child;
    private Cuboid region;//当前此网格的范围（分区网格范围）
    private Cuboid totalRegion; //所有网格的整体范围（全局网格范围）
    private double cellSideLength;


    //记录八叉树节点对应的网格单元
    private Map<String, HashSet<Long>> nodeCellsMap = new HashMap<>();
    //记录八叉树节点对应的网格单元存储的点总数量
    private HashMap<String, Long> nodeElementNumMap = new HashMap<>();
    //记录每个节点对应的最大网格单元数（用于判断节点分裂时与nodeElementNum进行比较）
    public HashMap<String, Long> nodeMaxCellNumMap = new HashMap<>();
    //记录分裂过的八叉树节点
    private Set<String> splitNodesSet = new HashSet<>();

    //储存网格单元及其包含点要素
    private Map<Long, List<Point3D>> cellElementsMap = new HashMap<>();


    public Grid3DLayer(Cuboid region, double cellSideLength, Cuboid totalRegion){
        this(region, cellSideLength ,0, totalRegion);
    }

    public Grid3DLayer(Cuboid region, double cellSideLength , int level, Cuboid totalRegion){
        this.region = region;
        this.totalRegion = totalRegion;
        this.cellSideLength = cellSideLength;
        this.cellNumXSide = (int)Math.ceil(region.getXSideLength() / cellSideLength);
        this.cellNumYSide = (int)Math.ceil(region.getYSideLength() / cellSideLength);
        this.cellNumZSide = (int)Math.ceil(region.getZSideLength() / cellSideLength);
        this.maxCellNum = cellNumXSide * cellNumYSide * cellNumZSide;
        this.level = level;
        this.child = null;
    }

    public void insert(Point3D pointToInsert){
        Preconditions.checkArgument(region.contains(pointToInsert), "the insert point is not in the grid region！");

        //添加网格单元
        Long gridCellKey = getGridCellKey(pointToInsert);
        cellElementsMap.putIfAbsent(gridCellKey, new ArrayList<Point3D>());

        //添加八叉树节点对应的网格单元
        String nodeKey = SplitUtils.getOctreeNodeName(getCellCenter(gridCellKey), totalRegion.getBoundingBox(), level);
        nodeCellsMap.putIfAbsent(nodeKey, new HashSet<>());
        nodeCellsMap.get(nodeKey).add(gridCellKey);

        List<Point3D> cellElementsList = cellElementsMap.get(gridCellKey);

        /*

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

         */
        if(!splitNodesSet.contains(nodeKey)|| cellElementsList.size() ==0){
            cellElementsList.add(pointToInsert);
            //elementsNum++;
            nodeElementNumMap.putIfAbsent(nodeKey, 0L);
            nodeElementNumMap.put(nodeKey, nodeElementNumMap.get(nodeKey) + 1L);

            nodeMaySplit(nodeKey);
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
     * 当节点中的点数超过其包含的网格单元数，则将点向下渗透
     * @param nodeKey
     */
    public void nodeMaySplit(String nodeKey){
        long time = System.currentTimeMillis();
        if(getNodeElementsNum(nodeKey) > getNodeMaxCellNum(nodeKey)){

            splitNodesSet.add(nodeKey);//记录分裂过的节点
            createChildGrid3DLayerIfNull();

            Set<Long> cellKeys = nodeCellsMap.get(nodeKey);
            for(Long cellKey : cellKeys){
                List<Point3D> cellElements = cellElementsMap.get(cellKey);
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
            nodeElementNumMap.put(nodeKey, (long) cellKeys.size());

        }
        /*
        if(System.currentTimeMillis() - time>10){
            System.out.println("分裂消耗时间："+ (System.currentTimeMillis()-time));
        }

         */

    }


    public long getNodeElementsNum(String nodeKey){
        return nodeElementNumMap.get(nodeKey);

    }


    /**
     * 查找当前网格层上对应八叉树节点所包含的最大网格单元数量
     * @param nodeKey
     * @return
     */
    public Long getNodeMaxCellNum(String nodeKey){
        if(nodeMaxCellNumMap.containsKey(nodeKey)){
            return nodeMaxCellNumMap.get(nodeKey);
        }

        double[] nodeBoundingBox = SplitUtils.getNodeBoundingBox(nodeKey, totalRegion.getBoundingBox());
        Cuboid intersectedRegion = region.intersectedRegion(new Cuboid(nodeBoundingBox[minX],nodeBoundingBox[minY],nodeBoundingBox[minZ],
                nodeBoundingBox[maxX],nodeBoundingBox[maxY],nodeBoundingBox[maxZ])).orNull();
        long nodeMaxCellNum = (long) (Math.ceil(intersectedRegion.getXSideLength()/ cellSideLength) *
                Math.ceil(intersectedRegion.getYSideLength()/ cellSideLength)*
                Math.ceil(intersectedRegion.getZSideLength()/ cellSideLength));
        nodeMaxCellNumMap.put(nodeKey, nodeMaxCellNum );
        return nodeMaxCellNum;
    }


    /**
     * 如果网格中点数量超过网格单元数量，则将网格中的点向下渗透
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
     */

    public void createChildGrid3DLayerIfNull(){
        if (child == null){
            child = createChildGrid3DLayer();
        }
    }

    private Grid3DLayer createChildGrid3DLayer(){
        return new Grid3DLayer(region, cellSideLength / 2.0 , level + 1, totalRegion);
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

    public long getElementsNum(){
        return nodeElementNumMap.values().stream().mapToLong(i-> i.longValue()).sum();
    }

    public boolean isLeaf(){
        return child == null;
    }

    public long getMaxCellNum() {
        return maxCellNum;
    }

    public int getGridLevel() {
        return level;
    }

    public Cuboid getGridRegion() {
        return region;
    }

    public double getCellSideLength() {
        return cellSideLength;
    }

    public Map<Long, List<Point3D>> getCellElementsMap() {
        return cellElementsMap;
    }

    public Cuboid getTotalRegion() {
        return totalRegion;
    }

    public Map<String, HashSet<Long>> getNodeCellsMap() {
        return nodeCellsMap;
    }

    public HashMap<String, Long> getNodeElementNumMap() {
        return nodeElementNumMap;
    }

    public HashMap<String, Long> getNodeMaxCellNumMap() {
        return nodeMaxCellNumMap;
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
