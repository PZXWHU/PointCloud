package com.pzx.geom;

import com.vividsolutions.jts.geom.Point;

import java.io.Serializable;

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
