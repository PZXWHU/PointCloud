package com.pzx.spatialPartition;

import com.pzx.geometry.Cuboid;
import com.pzx.geometry.Point3D;
import com.pzx.geometry.WithCuboidMBR;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class OcTreePartitioning implements Serializable {

    private OcTree<WithCuboidMBR> ocTree;

    public OcTreePartitioning(List<? extends WithCuboidMBR> samples, Cuboid boundary, int partitions){

        // Make sure the tree doesn't get too deep in case of data skew
        int maxLevel = partitions;
        int maxElementsPerNode = samples.size() / partitions;
        ocTree = new OcTree<>(boundary, maxElementsPerNode , maxLevel);

        for(WithCuboidMBR sample : samples){
            ocTree.insert(sample);
        }

    }

    public OcTreePartitioner getPartitioner(){
        ocTree.clearAllElements();
        return new OcTreePartitioner(ocTree);
    }

    private OcTree<? extends WithCuboidMBR> getOcTree(){return this.ocTree;}



}
