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
        return new OcTreePartitioner(ocTree);
    }

    public OcTree<? extends WithCuboidMBR> getOcTree(){return this.ocTree;}


    public static void main(String[] args) {
        List<Point3D> point3DS = new ArrayList<>();
        for(int i =0 ; i<1000000 ; i++){
            point3DS.add(new Point3D(Math.random()*100,Math.random()*100,Math.random()*100));
        }
        OcTreePartitioning ocTreePartitioning = new OcTreePartitioning(point3DS,new Cuboid(0,0,0,100,100,100),48);
        OcTreePartitioner ocTreePartitioner = ocTreePartitioning.getPartitioner();

        System.out.println(ocTreePartitioning.getOcTree().queryContains(new Cuboid(0,0,0,10,10,10)));

    }

}
