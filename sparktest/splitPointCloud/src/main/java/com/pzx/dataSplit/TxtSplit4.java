package com.pzx.dataSplit;

import com.google.common.base.Preconditions;
import com.pzx.geometry.Cuboid;
import com.pzx.geometry.Point3D;
import com.pzx.index.ocTree.OcTree;
import com.pzx.utils.NormalVectorUtils;
import com.pzx.utils.SizeOfObject;
import com.pzx.utils.SparkUtils;
import com.sun.org.glassfish.external.amx.AMX;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Serializable;
import scala.Tuple2;
import scala.Tuple3;
import scala.reflect.ClassManifestFactory;

import java.util.*;

/**
 * 八叉树分区 + 计算法向量（顺序加载）
 *
 * 效率比空间索引的方式慢很多，原因在于：
 * 未考虑网格切分数据，按照网格加载数据，当计算某网格时，需要扫描八邻域网格中的所有点。如果八邻域网格中的点特别多的话，造成循环耗时。
 * 而空间索引，比如八叉树，会将数据稠密的地方划分较大层级，所以查询时，遍历的点较少，所以速度较快。
 * 而顺序加载的方式，是以一个较大的间隔的网格进行数据划分，每个网格中的数据量有可能非常大。
 *
 *
 * 改进设想：
 * 把加载进来的点构造成八叉树，当有点有八叉树中淘汰时，则从八叉树中删除该点
 */
public class TxtSplit4 extends ComputeNormalVector implements Serializable {

    public long sizeOfBuffer = 0;

    private static Logger logger = LoggerFactory.getLogger(TxtSplit4.class);

    public TxtSplit4(String inputDirPath, String outputDirPath) {
        super(inputDirPath, outputDirPath);
    }

    @Override
    protected void splitData() {

        //空间分区
        JavaPairRDD<Tuple2<Integer,Long>, Point3D> partitionedRDDWithPartitionIDAndLoadOrder = spatialPartitioning();
        //分区裁剪
        JavaPairRDD<Tuple2<Integer,Long>, Point3D> prunedRDDWithOriginalPartitionIDAndLoadOrder =partitionsPruning(partitionedRDDWithPartitionIDAndLoadOrder);

        JavaRDD<Point3D> pointsRDDWithOriginalPartitionID = computeNormalVector(prunedRDDWithOriginalPartitionIDAndLoadOrder);

        writePointsToLocalFile(pointsRDDWithOriginalPartitionID, "D:\\wokspace\\点云数据集\\大数据集与工具\\data\\新建文件夹\\loadOrder\\");

    }

    /**
     * 利用八叉树对RDD进行分区

     * @return
     */
    protected JavaPairRDD<Tuple2<Integer,Long>, Point3D> spatialPartitioning(){

        class Tuple2Comparator implements Comparator<Tuple2<Integer, Long>>, Serializable{
            @Override
            public int compare(Tuple2<Integer, Long> o1, Tuple2<Integer, Long> o2) {
                return Long.compare(o1._2, o2._2);
            }
        }

        partitioner = getPartitioner(rowDataSet, sampleFraction);

        JavaRDD<Point3D> pointJavaRDD = rowDataSet.map((MapFunction<Row, Point3D>) row->rowToPoint3D(row), Encoders.kryo(Point3D.class)).toJavaRDD();

        //将knn转换为范围查询，根据平均密度决定范围查询的长度
        double pointDensity = pointCloud.points / pointCloud.tightBoundingBox.getVolume();
        searchCuboidLength = Math.pow(k / pointDensity, 1 / 3.0);


        JavaPairRDD<Tuple2<Integer,Long>, Point3D> partitionedRDDWithPartitionIDAndLoadOrder = pointJavaRDD.mapPartitionsToPair(pointIterator -> {
            List<Tuple2<Tuple2<Integer,Long>, Point3D>> result = new ArrayList<>();
            List<Cuboid> partitionRegions = partitioner.getPartitionRegions();

            while (pointIterator.hasNext()){
                Point3D point3D = pointIterator.next();
                Cuboid searchCuboid = Cuboid.createFromCenterPointAndSideLength(point3D, searchCuboidLength, searchCuboidLength, searchCuboidLength);
                //以搜索半径作为缓冲区半径（长方体缓冲区），将点冗余到多个分区中，并且注明点的notLocal的属性
                List<Integer> partitionIDs = partitioner.findPartitionIDs(searchCuboid);

                for(Integer partitionID : partitionIDs){
                    long loadOrder = getLoadOrder(partitionRegions.get(partitionID), point3D, searchCuboidLength);
                    if (!partitionRegions.get(partitionID).contains(point3D)){
                        Point3D outOfPartitionRegionPoint = point3D.copy();
                        outOfPartitionRegionPoint.setIsLocalPartition(false);
                        result.add(new Tuple2<Tuple2<Integer, Long>, Point3D>(new Tuple2<>(partitionID, loadOrder), outOfPartitionRegionPoint));//数据冗余
                    }else
                        result.add(new Tuple2<Tuple2<Integer, Long>, Point3D>(new Tuple2<>(partitionID, loadOrder), point3D));//数据冗余

                }
            }
            return result.iterator();
        }).repartitionAndSortWithinPartitions(partitioner, new Tuple2Comparator());
        return partitionedRDDWithPartitionIDAndLoadOrder;
    }


    protected JavaRDD<Point3D> computeNormalVector(JavaPairRDD<Tuple2<Integer,Long>, Point3D> prunedRDDWithOriginalPartitionIDAndLoadOrder) {

        //广播变量
        Broadcast<List<Cuboid>> partitionRegionsBroadcast = sparkSession.sparkContext().broadcast(partitioner.getPartitionRegions(), ClassManifestFactory.classType(List.class));

        return prunedRDDWithOriginalPartitionIDAndLoadOrder.mapPartitions(tupleIterator -> {
            List<Point3D> resultList = new ArrayList<>();

            Tuple2<Tuple2<Integer,Long>, Point3D> tuple = tupleIterator.next();
            int originalPartitionID = tuple._1._1;
            long loadOrder = tuple._1._2;
            Point3D point = tuple._2;

            Cuboid partitionRegion = partitionRegionsBroadcast.getValue().get(originalPartitionID);
            long gridNumberOneRow = Math.round(partitionRegion.getXSideLength() / searchCuboidLength) + 2;
            long lastLoadOrder = 0;
            long lastComputingGridIndex = -1;
            long lastEliminatingGridIndex = -1;

            HashMap<Long, Set<Point3D>> buffer = new HashMap<>();
            buffer.putIfAbsent(loadOrder, new HashSet<>());
            buffer.get(loadOrder).add(point);

            while (tupleIterator.hasNext() ){

                tuple = tupleIterator.next();
                loadOrder = tuple._1._2;
                point = tuple._2;

                buffer.putIfAbsent(loadOrder, new HashSet<>());
                buffer.get(loadOrder).add(point);

                if (loadOrder == lastLoadOrder )
                    continue;

                //计算
                List<Long> computingGridIndexList = getComputingGridIndex(loadOrder - 1, gridNumberOneRow);
                if (!computingGridIndexList.isEmpty()){
                    long currentMaxComputingGridIndex = computingGridIndexList.get(computingGridIndexList.size() - 1);
                    for(long i = lastComputingGridIndex + 1; i <= currentMaxComputingGridIndex; i++){
                        resultList.addAll(computeGrid(i,gridNumberOneRow, buffer));
                    }
                    lastComputingGridIndex = currentMaxComputingGridIndex;

                }

                //删除缓存
                List<Long> eliminatingGridIndexList = getEliminatingGridIndex(loadOrder - 1, gridNumberOneRow);
                if (!eliminatingGridIndexList.isEmpty()){
                    long currentMaxEliminatingGridIndex = eliminatingGridIndexList.get(eliminatingGridIndexList.size() - 1);
                    for (long i = lastEliminatingGridIndex + 1; i <= currentMaxEliminatingGridIndex; i++){
                        buffer.remove(i);
                    }
                    lastEliminatingGridIndex = currentMaxEliminatingGridIndex;
                }

                lastLoadOrder = loadOrder;
            }

            long maxIndex = gridNumberOneRow * gridNumberOneRow * gridNumberOneRow;
            resultList.addAll(computeAndEliminateRemainGrid(lastComputingGridIndex, lastEliminatingGridIndex, maxIndex, gridNumberOneRow, buffer));
            return resultList.iterator();
        });

    }

    /**
     * 将剩下的网格全部计算并删除缓存
     * @param lastComputingGridIndex
     * @param lastEliminatingGridIndex
     * @param maxIndex
     * @param gridNumberOneRow
     * @param buffer
     * @return
     */
    private List<Point3D> computeAndEliminateRemainGrid(long lastComputingGridIndex, long lastEliminatingGridIndex, long maxIndex,
                                                     long gridNumberOneRow, HashMap<Long, Set<Point3D>> buffer){
        List<Point3D> resultList = new ArrayList<>();
        for(long i = lastComputingGridIndex + 1; i <= maxIndex; i++){
            resultList.addAll(computeGrid(i,gridNumberOneRow, buffer));
        }
        for (long i = lastEliminatingGridIndex + 1; i <= maxIndex; i++){
            buffer.remove(i);
        }
        return resultList;
    }


    /**
     * 计算网格中的所有点，并删除缓存
     * @param computingGridIndex
     * @param gridNumberOneRow
     * @param buffer
     * @return
     */
    private Set<Point3D> computeGrid(long computingGridIndex,long gridNumberOneRow, HashMap<Long, Set<Point3D>> buffer){

        Set<Point3D> computingPointSet = buffer.get(computingGridIndex);

        if (computingPointSet == null || computingPointSet.isEmpty()){
            return Collections.emptySet();
        }

        Set<Set<Point3D>> neighborPointSets = new HashSet<>();

        for(long neighborGridIndex : getNeighborGridIndex(computingGridIndex, gridNumberOneRow)){
            neighborPointSets.add(buffer.getOrDefault(neighborGridIndex, Collections.emptySet()));
        }

        Cuboid searchCuboid = Cuboid.createFromMinAndMaxCoordinate(0,0,0,0,0,0);
        Set<Point3D> resultSet = new HashSet<>();
        for(Point3D computingPoint : computingPointSet){
            if (computingPoint.isLocalPartition()){
                searchCuboid.setMinX(computingPoint.x - searchCuboidLength / 2);
                searchCuboid.setMinY(computingPoint.y - searchCuboidLength / 2);
                searchCuboid.setMinZ(computingPoint.z - searchCuboidLength / 2);
                searchCuboid.setMaxX(computingPoint.x + searchCuboidLength / 2);
                searchCuboid.setMaxY(computingPoint.y + searchCuboidLength / 2);
                searchCuboid.setMaxZ(computingPoint.z + searchCuboidLength / 2);
                List<Point3D> searchedPoints = new ArrayList<>();
                for (Set<Point3D> neighborPointSet : neighborPointSets){
                    for (Point3D neighborPoint : neighborPointSet){
                        if (searchCuboid.contains(neighborPoint))
                            searchedPoints.add(neighborPoint);
                    }
                }
                computingPoint.setNormalVector(NormalVectorUtils.getNormalVector(searchedPoints, k));
                resultSet.add(computingPoint);
            }
        }

        return resultSet;


    }

    /**
     * 获得十进制index的邻域index（将十进制index转换为coordinate，然后获取其邻域的coordinate，然后获得邻域的index）
     * @param centerGridIndex
     * @param gridNumberOneRow
     * @return
     */
    private List<Long> getNeighborGridIndex(long centerGridIndex,long gridNumberOneRow){

        int dimension = 3;
        List<Long> neighborGridIndexList = new ArrayList<>();
        long[] centerGridCoordinate = new long[3];
        for (int i = 0; i < dimension; i++){
            centerGridCoordinate[i] = centerGridIndex % gridNumberOneRow;
            centerGridIndex = centerGridIndex / gridNumberOneRow;
        }
        //long [] neighborGridCoordinate = new long[dimension];
        int[] bias = new int[]{-1,0,1};

        for(int i = 0; i < Math.pow(3,dimension); i++) {
            if (i == (int)Math.pow(3, dimension) / 2)
                continue;

            long neighborGridIndex = 0;
            int ternary = i;
            for (int j = 0; j < dimension; j++) {
                //neighborGridCoordinate[j] = centerGridCoordinate[j] + bias[ternary % 3];
                neighborGridIndex +=  (centerGridCoordinate[j] + bias[ternary % 3]) * Math.pow(gridNumberOneRow, j);
                ternary = ternary / 3;
            }
            neighborGridIndexList.add(neighborGridIndex);

        }
        return neighborGridIndexList;
    }

    /**
     * 获取需要删除的网格单元index
     * @param loadOrder 第loadOrder个网格被加载
     * @param gridNumberOneRow 某个坐标轴方向上网格的个数
     * @return
     */
    private List<Long> getEliminatingGridIndex(long loadOrder, long gridNumberOneRow ){

        long eliminatingGridIndex = loadOrder - 2 * gridNumberOneRow * gridNumberOneRow - 2 * gridNumberOneRow - 2;

        if (eliminatingGridIndex < 0 || loadOrder % gridNumberOneRow < 2)
            return Collections.emptyList();
        else if (loadOrder % gridNumberOneRow != gridNumberOneRow - 1)
            return Collections.singletonList(eliminatingGridIndex);
        else
            return Arrays.asList(eliminatingGridIndex, eliminatingGridIndex + 1, eliminatingGridIndex + 2);

    }

    /**
     * 获取需要计算的网格单元index
     * @param loadOrder 第loadOrder个网格被加载
     * @param gridNumberOneRow 某个坐标轴方向上网格的个数
     * @return
     */
    private List<Long> getComputingGridIndex(long loadOrder, long gridNumberOneRow ){

        long computingGridIndex = loadOrder - gridNumberOneRow * gridNumberOneRow - gridNumberOneRow - 1;

        if (computingGridIndex < 0 || computingGridIndex % gridNumberOneRow < 1)
            return Collections.emptyList();
        else if (computingGridIndex % gridNumberOneRow != gridNumberOneRow - 1)
            return Collections.singletonList(computingGridIndex);
        else
            return Arrays.asList(computingGridIndex, computingGridIndex + 1);

    }


    /**
     * 获取一个点在分区上的加载顺序
     * @param partitionRegion
     * @param point3D
     * @param gridCellLen
     * @return
     */
    private long getLoadOrder(Cuboid partitionRegion, Point3D point3D, double gridCellLen){
        Cuboid expandCuboid = partitionRegion.expand(gridCellLen);//因为数据冗余，所以需要扩大分区的范围来决定数据加载顺序

        Preconditions.checkArgument(expandCuboid.contains(point3D), "the expand partition do not contains the point");

        int xIndex = (int)((point3D.x - expandCuboid.getMinX()) / gridCellLen);
        int yIndex = (int)((point3D.y - expandCuboid.getMinY()) / gridCellLen);
        int zIndex = (int)((point3D.z - expandCuboid.getMinZ()) / gridCellLen);

        long xGridCellNum = Math.round(expandCuboid.getXSideLength() / gridCellLen);
        long yGridCellNum = Math.round(expandCuboid.getYSideLength() / gridCellLen);
        long zGridCellNum = Math.round(expandCuboid.getZSideLength() / gridCellLen);

        long order = zIndex * xGridCellNum * yGridCellNum + yIndex * xGridCellNum + xIndex;

        return order;

    }

    @Override
    protected void sparkSessionInit() {
        sparkSession = SparkUtils.localSparkSessionInit();
    }

    public static void main(String[] args) {

        //输入本地输入路径
        String inputDirPath = args[0];
        //生成结果输出路径
        String outputDirPath = args[1];

        TxtSplit4 splitter = new TxtSplit4(inputDirPath, outputDirPath);
        splitter.dataSplit();


    }

}
