package com.pzx.spatialPartition;

import com.google.common.base.Preconditions;
import com.pzx.geom.Cuboid;
import com.pzx.geom.WithCuboidMBR;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import org.apache.spark.Partitioner;

import scala.Serializable;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OcTreePartitioner extends Partitioner implements Serializable {

    private OcTree<? extends WithCuboidMBR> ocTree;
    private List<Cuboid> partitionRegions;
    private HashMap<Cuboid, Integer> partitionRegionIDMap = new HashMap<>();

    public OcTreePartitioner(OcTree<? extends WithCuboidMBR> ocTree) {
        this.ocTree = ocTree;
        this.partitionRegions = ocTree.getAllLeafNodeRegion();
        long time = System.currentTimeMillis();
        for(int partitionID =0 ; partitionID<partitionRegions.size(); partitionID++){
            partitionRegionIDMap.put(partitionRegions.get(partitionID), partitionID);
        }
        System.out.println("构造需时间："+(System.currentTimeMillis()-time));
    }

    /**
     * 为每一个空间对象生成分区id
     * @param spatialObject
     * @param <T>
     * @return
     */
    public <T extends WithCuboidMBR> Tuple2<Integer, T> placeObject(T spatialObject){
        Preconditions.checkNotNull(spatialObject);
        //用MBR中心点获得所属分区，保证只属于一个分区
        List<Cuboid> resultRegions = ocTree.findLeafNodeRegion(spatialObject.getCuboidMBR().centerPoint());
        Integer partitionID = partitionRegionIDMap.get(resultRegions.get(0));
        if (partitionID == null)
            throw new RuntimeException("can not find partition for the spatialObject!");
        return new Tuple2<Integer, T>(partitionID, spatialObject);
    }

    @Override
    public int getPartition(Object key) { return (int)key; }

    @Override
    public int numPartitions() {
        return partitionRegions.size();
    }

    public void printPartition(){
        for(Map.Entry<Cuboid, Integer> entry : partitionRegionIDMap.entrySet()){
            System.out.println("partitionID: " + entry.getValue() + " , partitionRegion: "+ entry.getKey());
        }
    }
}
