package com.pzx.spatialPartition;

import com.pzx.geometry.Cuboid;
import com.pzx.geometry.MinimumBoundingBox;
import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.List;

public abstract class CustomPartitioner extends Partitioner implements Serializable {

    abstract public <T extends MinimumBoundingBox> List<Integer> findPartitionIDs(T object);

    abstract public List<Cuboid> getPartitionRegions();

    abstract public Cuboid getTotalRegion();

}
