package com.pzx.spatialPartition;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import org.apache.spark.Partitioner;

public class OctreePartitioner extends Partitioner {

    public OctreePartitioner() {
        super();


    }

    @Override
    public int getPartition(Object key) {
        return 0;
    }

    @Override
    public int numPartitions() {
        return 0;
    }
}
