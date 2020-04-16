package com.pzx.pointcloud;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PointCloud implements Serializable {

    public long pointsNum;
    public double[] tightBoundingBox;//maxx, maxy, maxz,minx,miny,minz
    public double[] boundingBox;//maxx, maxy, maxz,minx,miny,minz
    public List<PointAttribute> pointAttributes;
    public double scale;
    public long pointNumPerNode;
    public int dimension;
    public int maxLevel;


    public PointCloud(){
        tightBoundingBox = new double[6];
        boundingBox = new double[6];
        pointAttributes = new ArrayList<>();
    }

    public long getPointsNum() {
        return pointsNum;
    }

    public void setPointsNum(long pointsNum) {
        this.pointsNum = pointsNum;
    }

    public double[] getTightBoundingBox() {
        return tightBoundingBox;
    }

    public void setTightBoundingBox(double[] tightBoundingBox) {
        this.tightBoundingBox = tightBoundingBox;
    }

    public double[] getBoundingBox() {
        return boundingBox;
    }

    public void setBoundingBox(double[] boundingBox) {
        this.boundingBox = boundingBox;
    }

    public List<PointAttribute> getPointAttributes() {
        return pointAttributes;
    }

    public void setPointAttributes(List<PointAttribute> pointAttributes) {
        this.pointAttributes = pointAttributes;
    }

    public double getScale() {
        return scale;
    }

    public void setScale(double scale) {
        this.scale = scale;
    }

    public long getPointNumPerNode() {
        return pointNumPerNode;
    }

    public void setPointNumPerNode(long pointNumPerNode) {
        this.pointNumPerNode = pointNumPerNode;
    }

    public int getDimension() {
        return dimension;
    }

    public void setDimension(int dimension) {
        this.dimension = dimension;
    }

    public int getMaxLevel() {
        return maxLevel;
    }

    public void setMaxLevel(int maxLevel) {
        this.maxLevel = maxLevel;
    }


}
