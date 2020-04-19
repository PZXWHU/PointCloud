package com.pzx.geometry;



import java.io.Serializable;

public class Cube extends Cuboid implements Serializable {

    public Cube(double minX, double minY, double minZ, double sideLength){
        super(new Point3D(minX,minY,minZ),sideLength,sideLength,sideLength);
    }

}
