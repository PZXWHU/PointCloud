package com.pzx.geometry;



import java.io.Serializable;

public class Cube extends Cuboid implements Serializable {

    private double sideLength;

    public Cube(double minX, double minY, double minZ, double sideLength){
        this.setMinX(minX);
        this.setMinY(minY);
        this.setMinZ(minZ);
        this.setMaxX(minX + sideLength);
        this.setMaxY(minY + sideLength);
        this.setMaxZ(minZ + sideLength);
        this.sideLength = sideLength;
    }

    public double getSideLength(){return this.sideLength;}

}
