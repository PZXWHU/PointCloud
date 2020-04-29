package com.pzx.pointCloud;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.pzx.geometry.Cube;
import com.pzx.geometry.Cuboid;


import java.io.Serializable;
import java.util.List;

public class PointCloud implements Serializable {

    public long points;
    public Cuboid tightBoundingBox;//maxx, maxy, maxz,minx,miny,minz
    public Cube boundingBox;//maxx, maxy, maxz,minx,miny,minz
    public List<PointAttribute> pointAttributes;
    public double[] scales;
/*
    public static int maxX = 0;
    public static int maxY = 1;
    public static int maxZ = 2;
    public static int minX = 3;
    public static int minY = 4;
    public static int minZ = 5;

 */

    public PointCloud(){

    }

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

    public void setTightBoundingBox(Cuboid tightBoundingBox) {
        this.tightBoundingBox = tightBoundingBox;
    }

    public Cube getBoundingBox() {
        return boundingBox;
    }

    public void setBoundingBox(Cube boundingBox) {
        this.boundingBox = boundingBox;
    }

    public List<PointAttribute> getPointAttributes() {
        return pointAttributes;
    }

    public void setPointAttributes(List<PointAttribute> pointAttributes) {
        this.pointAttributes = pointAttributes;
    }

    public long getPoints() { return points; }

    public void setPoints(long points) { this.points = points; }

    public double[] getScales() { return scales; }

    public void setScales(double[] scales) { this.scales = scales; }
}
