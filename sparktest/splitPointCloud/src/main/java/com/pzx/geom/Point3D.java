package com.pzx.geom;

import com.vividsolutions.jts.geom.Point;

public class Point3D implements WithCuboidMBR {

    public double x;
    public double y;
    public double z;

    private Cuboid mbr;

    public Point3D(double x , double y , double z){
        this.x = x;
        this.y = y;
        this.z = z;
        mbr = new Cuboid(x,y,z,x,y,z);
    }

    @Override
    public Cuboid getCuboidMBR() {
        return mbr;
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
