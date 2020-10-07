package com.pzx.spatialPartition;

import com.google.common.base.Preconditions;
import com.pzx.geometry.Cuboid;
import com.pzx.geometry.Point3D;
import com.pzx.geometry.MinimumBoundingBox;
import com.pzx.index.ocTree.OcTree;
import org.apache.spark.Partitioner;


import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OcTreePartitioner extends CustomPartitioner{

    private OcTree<? extends MinimumBoundingBox> ocTree;
    private List<Cuboid> partitionRegions;
    private HashMap<Cuboid, Integer> partitionRegionIDMap = new HashMap<>();

    public OcTreePartitioner(OcTree<? extends MinimumBoundingBox> ocTree) {
        this.ocTree = ocTree;
        this.partitionRegions = ocTree.getAllLeafNodeRegions();
        for(int partitionID =0 ; partitionID<partitionRegions.size(); partitionID++){
            partitionRegionIDMap.put(partitionRegions.get(partitionID), partitionID);
        }
    }

    /**
     * 为每一个空间对象生成分区id
     * @param object
     * @param
     * @return
     */
    public <T extends MinimumBoundingBox>  List<Integer> findPartitionIDs(T object){
        Preconditions.checkNotNull(object);

        List<Cuboid> resultRegions = ocTree.queryLeafNodeRegions(object);
        List<Integer> partitionIDs = new ArrayList<>();
        for(Cuboid partitionRegion : resultRegions){
            partitionIDs.add(partitionRegionIDMap.get(partitionRegion));
        }

        if (partitionIDs.size() == 0)
            throw new RuntimeException("can not find partition for the spatialObject!");
        return partitionIDs;
    }


    @Override
    public int getPartition(Object key) {
        if (key instanceof Integer)
            return (int)key;
        else if (key instanceof Tuple2)
            return (int)((Tuple2) key)._1;
        else
            throw new IllegalArgumentException(key + " is not correct");

    }

    @Override
    public int numPartitions() {
        return partitionRegions.size();
    }

    @Override
    public List<Cuboid> getPartitionRegions(){ return this.partitionRegions;}

    @Override
    public Cuboid getTotalRegion(){return ocTree.getRegion();}

    public void printPartition(){
        for(Map.Entry<Cuboid, Integer> entry : partitionRegionIDMap.entrySet()){
            System.out.println("partitionID: " + entry.getValue() + " , partitionRegion: "+ entry.getKey());
        }
    }
}
