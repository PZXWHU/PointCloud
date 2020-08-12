package com.pzx.spatialPartition;

import com.pzx.geometry.Cuboid;
import com.pzx.geometry.MinimumBoundingBox;
import com.pzx.index.ocTree.OcTree;

import java.io.Serializable;
import java.util.List;

public class OcTreePartitioning implements Serializable {

    private OcTree<MinimumBoundingBox> ocTree;

    public OcTreePartitioning(List<? extends MinimumBoundingBox> samples, Cuboid boundary, int partitions){

        // Make sure the tree doesn't get too deep in case of data skew
        int maxLevel = partitions;
        int maxElementsPerNode = samples.size() / partitions;
        ocTree = new OcTree<>(boundary, maxElementsPerNode , maxLevel);

        for(MinimumBoundingBox sample : samples){
            ocTree.insert(sample);
        }

    }

    public OcTreePartitioner getPartitioner(){
        ocTree.clearAllElements();
        return new OcTreePartitioner(ocTree);
    }

    private OcTree<? extends MinimumBoundingBox> getOcTree(){return this.ocTree;}



}
