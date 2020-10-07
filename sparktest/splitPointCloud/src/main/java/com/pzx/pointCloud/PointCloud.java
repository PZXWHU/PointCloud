package com.pzx.pointCloud;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.pzx.geometry.Cube;
import com.pzx.geometry.Cuboid;


import java.io.Serializable;
import java.util.List;

public class PointCloud implements Serializable {

    public final long points;
    public final Cuboid tightBoundingBox;//maxx, maxy, maxz,minx,miny,minz
    public final Cube boundingBox;//maxx, maxy, maxz,minx,miny,minz
    public final List<PointAttribute> pointAttributes;
    public final double[] scales;

    public PointCloud(long points, Cuboid tightBoundingBox, List<PointAttribute> pointAttributes, double[] scales) {
        this.points = points;
        this.tightBoundingBox = tightBoundingBox;
        this.boundingBox = createBoundingBox(tightBoundingBox);
        this.pointAttributes = pointAttributes;
        this.scales = scales;
    }

    /**
     * 由tightBoundingBox生成boundingBox
     * @param tightBoundingBox
     * @return
     */
    public static Cube createBoundingBox(Cuboid tightBoundingBox){

        double maxSideLength = 0.0;
        maxSideLength = Math.max(maxSideLength,tightBoundingBox.getXSideLength());
        maxSideLength = Math.max(maxSideLength,tightBoundingBox.getYSideLength());
        maxSideLength = Math.max(maxSideLength,tightBoundingBox.getZSideLength());
        return new Cube(tightBoundingBox.getMinX(), tightBoundingBox.getMinY(), tightBoundingBox.getMinZ(), maxSideLength);
    }

    public JSONObject buildCloudJS(){
        JSONObject cloudjs = new JSONObject();
        cloudjs.put("points",points);

        JSONObject tightBoundingBoxJson = new JSONObject();
        tightBoundingBoxJson.put("ux",tightBoundingBox.getMaxX());
        tightBoundingBoxJson.put("uy",tightBoundingBox.getMaxY());
        tightBoundingBoxJson.put("uz",tightBoundingBox.getMaxZ());
        tightBoundingBoxJson.put("lx",tightBoundingBox.getMinX());
        tightBoundingBoxJson.put("ly",tightBoundingBox.getMinY());
        tightBoundingBoxJson.put("lz",tightBoundingBox.getMinZ());
        cloudjs.put("tightBoundingBox",tightBoundingBoxJson);

        JSONObject boundingBoxJson = new JSONObject();
        boundingBoxJson.put("ux",boundingBox.getMaxX());
        boundingBoxJson.put("uy",boundingBox.getMaxY());
        boundingBoxJson.put("uz",boundingBox.getMaxZ());
        boundingBoxJson.put("lx",boundingBox.getMinX());
        boundingBoxJson.put("ly",boundingBox.getMinY());
        boundingBoxJson.put("lz",boundingBox.getMinZ());
        cloudjs.put("boundingBox",boundingBoxJson);

        cloudjs.put("scale",scales);

        cloudjs.put("pointAttributes",pointAttributes);
        return cloudjs;
    }

    public Cuboid getTightBoundingBox() {
        return tightBoundingBox;
    }

    public Cube getBoundingBox() {
        return boundingBox;
    }

    public List<PointAttribute> getPointAttributes() {
        return pointAttributes;
    }

    public long getPoints() { return points; }

    public double[] getScales() { return scales; }


}
