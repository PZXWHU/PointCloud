package com.pzx.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.gson.JsonObject;

public class CloudJSUtils {

    /**
     * 由tightBoundingBox生成boundingBox
     * @param tightBoundingBox
     * @return
     */
    public static double[] getBoundingBox(double[] tightBoundingBox){
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

    public static JSONObject buildCloudJS(long points,double[] tightBoundingBox,double[] boundingBox,double[] scale,String[] pointAttributes ){
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

        cloudjs.put("scale",scale);

        cloudjs.put("pointAttributes",pointAttributes);
        return cloudjs;
    }


}
