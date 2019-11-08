package com;

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

    @Override
    public String toString() {


        String pointNumStr = " pointNum:" + pointsNum;
        String scaleStr = " scale:" + scale;

        String tightBoundingBoxStr = " tightBoundingBox: maxx:"+tightBoundingBox[0] +
                " maxy:"+tightBoundingBox[1] +
                " maxz:"+tightBoundingBox[2] +
                " minx:"+tightBoundingBox[3] +
                " miny:"+tightBoundingBox[4] +
                " minz:"+tightBoundingBox[5] ;
        String boundingBoxStr = " boundingBox: maxx:"+boundingBox[0] +
                " maxy:"+boundingBox[1] +
                " maxz:"+boundingBox[2] +
                " minx:"+boundingBox[3] +
                " miny:"+boundingBox[4] +
                " minz:"+boundingBox[5] ;



        String pointNumPerNodeStr = " pointNumPerNode:"+ pointNumPerNode;
        String dimensionStr = " dimension:" +dimension;
        String maxLevelStr = " maxLevel:" + maxLevel;

        String str = ""+pointNumStr+"\n"+scaleStr+"\n"+tightBoundingBoxStr+"\n"+boundingBoxStr+"\n"+pointNumPerNodeStr+"\n"+dimensionStr+"\n"+maxLevelStr;
        return str;
    }
}
