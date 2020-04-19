package com.pzx.pointCloud;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;


import java.io.Serializable;
import java.util.List;

public class PointCloud implements Serializable {

    public long points;
    public double[] tightBoundingBox;//maxx, maxy, maxz,minx,miny,minz
    public double[] boundingBox;//maxx, maxy, maxz,minx,miny,minz
    public List<PointAttribute> pointAttributes;
    public double[] scales;

    public static int maxX = 0;
    public static int maxY = 1;
    public static int maxZ = 2;
    public static int minX = 3;
    public static int minY = 4;
    public static int minZ = 5;

    public PointCloud(){

    }

    public PointCloud(long points, double[] tightBoundingBox, List<PointAttribute> pointAttributes, double[] scales) {
        Preconditions.checkArgument(tightBoundingBox[maxX]>=tightBoundingBox[minX] &&
                tightBoundingBox[maxY]>tightBoundingBox[minY] && tightBoundingBox[maxZ]>tightBoundingBox[minZ]);
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
    public static double[] createBoundingBox(double[] tightBoundingBox){
        Preconditions.checkArgument(tightBoundingBox.length%2 ==0,"tightBoundingBox数组长度必须是偶数");
        int length = tightBoundingBox.length;
        double[] boundingBox = new double[length];
        //boundingBox
        double boxSideLength = 0;
        for(int i=0;i<length/2;i++){
            boxSideLength = Math.max(tightBoundingBox[i]-tightBoundingBox[i+length/2],boxSideLength);
        }
        for(int i=0;i<length/2;i++){
            boundingBox[i+length/2] = tightBoundingBox[i+length/2];
            boundingBox[i] = boundingBox[i+length/2] +boxSideLength;
        }
        return boundingBox;
    }

    public JSONObject buildCloudJS(){
        JSONObject cloudjs = new JSONObject();
        cloudjs.put("points",points);

        JSONObject tightBoundingBoxJson = new JSONObject();
        tightBoundingBoxJson.put("ux",tightBoundingBox[0]);
        tightBoundingBoxJson.put("uy",tightBoundingBox[1]);
        tightBoundingBoxJson.put("uz",tightBoundingBox[2]);
        tightBoundingBoxJson.put("lx",tightBoundingBox[3]);
        tightBoundingBoxJson.put("ly",tightBoundingBox[4]);
        tightBoundingBoxJson.put("lz",tightBoundingBox[5]);
        cloudjs.put("tightBoundingBox",tightBoundingBoxJson);

        JSONObject boundingBoxJson = new JSONObject();
        boundingBoxJson.put("ux",boundingBox[0]);
        boundingBoxJson.put("uy",boundingBox[1]);
        boundingBoxJson.put("uz",boundingBox[2]);
        boundingBoxJson.put("lx",boundingBox[3]);
        boundingBoxJson.put("ly",boundingBox[4]);
        boundingBoxJson.put("lz",boundingBox[5]);
        cloudjs.put("boundingBox",boundingBoxJson);

        cloudjs.put("scale",scales);

        cloudjs.put("pointAttributes",pointAttributes);
        return cloudjs;
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

    public long getPoints() { return points; }

    public void setPoints(long points) { this.points = points; }

    public double[] getScales() { return scales; }

    public void setScales(double[] scales) { this.scales = scales; }
}
