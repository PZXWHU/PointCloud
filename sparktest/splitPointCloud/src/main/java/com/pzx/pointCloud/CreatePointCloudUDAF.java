package com.pzx.pointCloud;

import com.pzx.geometry.Cuboid;
import com.pzx.geometry.Point3D;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.util.Arrays;

public class CreatePointCloudUDAF extends Aggregator<Point3D, PointCloud, PointCloud> {
    @Override
    public PointCloud zero() {
        PointCloud pointCloud = new PointCloud();
        pointCloud.setPoints(0);
        pointCloud.setTightBoundingBox(new Cuboid(Double.MAX_VALUE,Double.MAX_VALUE,Double.MAX_VALUE,
                -Double.MAX_VALUE, -Double.MAX_VALUE, -Double.MAX_VALUE ));
        return pointCloud;
    }
    @Override
    public PointCloud reduce(PointCloud pointCloud, Point3D point3D) {
        pointCloud.setPoints(pointCloud.getPoints() + 1);
        Cuboid tightBoundingBox = pointCloud.getTightBoundingBox();

        tightBoundingBox.setMaxX(Math.max(point3D.x, tightBoundingBox.getMaxX()));
        tightBoundingBox.setMaxY(Math.max(point3D.y, tightBoundingBox.getMaxY()));
        tightBoundingBox.setMaxZ(Math.max(point3D.z, tightBoundingBox.getMaxZ()));
        tightBoundingBox.setMinX(Math.min(point3D.x,tightBoundingBox.getMinX()));
        tightBoundingBox.setMinY(Math.min(point3D.y,tightBoundingBox.getMinY()));
        tightBoundingBox.setMaxZ(Math.min(point3D.z,tightBoundingBox.getMinZ()));

        return pointCloud;
    }
    @Override
    public PointCloud merge(PointCloud pointCloud1, PointCloud pointCloud2) {
        pointCloud1.setPoints(pointCloud1.getPoints() + pointCloud2.getPoints());
        Cuboid tightBoundingBox1 = pointCloud1.getTightBoundingBox();
        Cuboid tightBoundingBox2 = pointCloud2.getTightBoundingBox();

        tightBoundingBox1.setMaxX(Math.max(tightBoundingBox1.getMaxX(), tightBoundingBox2.getMaxX()));
        tightBoundingBox1.setMaxY(Math.max(tightBoundingBox1.getMaxY(), tightBoundingBox2.getMaxY()));
        tightBoundingBox1.setMaxZ(Math.max(tightBoundingBox1.getMaxZ(), tightBoundingBox2.getMaxZ()));
        tightBoundingBox1.setMinX(Math.min(tightBoundingBox1.getMinX(), tightBoundingBox2.getMinX()));
        tightBoundingBox1.setMinY(Math.min(tightBoundingBox1.getMinY(), tightBoundingBox2.getMinY()));
        tightBoundingBox1.setMinZ(Math.min(tightBoundingBox1.getMinZ(), tightBoundingBox2.getMinZ()));

        return pointCloud1;
    }
    @Override
    public PointCloud finish(PointCloud pointCloud) {
        pointCloud.setBoundingBox(pointCloud.createBoundingBox(pointCloud.getTightBoundingBox()));
        pointCloud.setScales(new double[]{0.001,0.001,0.001});
        pointCloud.setPointAttributes(Arrays.asList(PointAttribute.POSITION_XYZ, PointAttribute.RGB));
        return pointCloud;
    }

    @Override
    public Encoder<PointCloud> bufferEncoder() { return Encoders.kryo(PointCloud.class); }

    @Override
    public Encoder<PointCloud> outputEncoder() { return Encoders.kryo(PointCloud.class); }
}
