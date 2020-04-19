package com.pzx.geometry;

import com.pzx.utils.SplitUtils;
import org.apache.commons.lang.ArrayUtils;

import java.io.Serializable;
import java.nio.Buffer;

public class Point3D implements WithCuboidMBR, Serializable {

    public double x;
    public double y;
    public double z;
    public byte r;
    public byte g;
    public byte b;



    public Point3D(double x , double y , double z){
        this.x = x;
        this.y = y;
        this.z = z;
        this.r = 0;
        this.g = 0;
        this.b = 0;

    }

    public Point3D(double x , double y , double z, byte r, byte g, byte b){
        this.x = x;
        this.y = y;
        this.z = z;
        this.r = r;
        this.g = g;
        this.b = b;

    }

    public double distance(Point3D point3D){
        return Math.sqrt(Math.pow((point3D.x - x),2) + Math.pow((point3D.y - y),2) + Math.pow((point3D.z - z),2));
    }

    public byte[] serialize(double[] xyzOffset, double scale[]){
        int newX = (int)((x-xyzOffset[0])/scale[0]);
        int newY = (int)((y-xyzOffset[1])/scale[1]);
        int newZ = (int)((z-xyzOffset[2])/scale[2]);
        byte[] coordinateBytes = SplitUtils.pointInZigZagFormat(new int[]{newX,newY,newZ});

        return ArrayUtils.addAll(coordinateBytes, new byte[]{r,g,b});
    }

    @Override
    public Cuboid getCuboidMBR() {
        return new Cuboid(x,y,z,x,y,z);
    }

    @Override
    public String toString() {
        return "Point3D{ " +
                 + x +
                ", " + y +
                ", " + z +
                '}';
    }
}
