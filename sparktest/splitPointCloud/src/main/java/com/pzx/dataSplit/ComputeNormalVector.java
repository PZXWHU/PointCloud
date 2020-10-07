package com.pzx.dataSplit;

import com.pzx.geometry.Cuboid;
import com.pzx.geometry.Point3D;
import com.pzx.index.ocTree.OcTree;
import com.pzx.pointCloud.PointCloud;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.*;

public abstract class ComputeNormalVector extends TxtSplitter {

    private final static Logger logger = LoggerFactory.getLogger(ComputeNormalVector.class);
    protected final int k = 20;
    protected double searchCuboidLength;//查询的范围大小

    public ComputeNormalVector(String inputDirPath, String outputDirPath) {
        super(inputDirPath, outputDirPath);
    }

    @Override
    abstract protected void splitData();

    @Override
    protected void createHrc() {

    }

    protected void writePointsToLocalFile(JavaRDD<Point3D> pointsRDDWithOriginalPartitionID, String outputDir){
        pointsRDDWithOriginalPartitionID.mapPartitionsWithIndex((partitionID, iterator) ->{

            if (!iterator.hasNext())
                return Collections.emptyIterator();
           Point3D point3D = iterator.next();

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(outputDir + partitionID+".txt")));
            writer.write(point3D.x + " "+ point3D.y + " "+ point3D.z + Arrays.toString(point3D.getNormalVector()));

            while (iterator.hasNext()){
                point3D = iterator.next();
                writer.newLine();
                writer.write(point3D.x + " "+ point3D.y + " "+ point3D.z + Arrays.toString(point3D.getNormalVector()));
            }
            writer.close();
            return Collections.emptyIterator();
        }, true).foreachPartition(iter ->{});
    }

}
