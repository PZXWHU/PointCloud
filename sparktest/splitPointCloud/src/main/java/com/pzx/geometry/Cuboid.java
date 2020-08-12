package com.pzx.geometry;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import java.io.Serializable;

public class Cuboid implements MinimumBoundingBox, Serializable {

    private final double minX;
    private final double minY;
    private final double minZ;
    private final double maxX;
    private final double maxY;
    private final double maxZ;


    public Cuboid(double minX, double minY, double minZ, double maxX, double maxY, double maxZ) {
        Preconditions.checkArgument(minX<=maxX && minY<=maxY && minZ<=maxZ,
                "the first three parameters {} {} {} must be smaller than the last three {} {} {}！"
                ,minX, minY, minZ, maxX ,maxY, maxZ);
        this.minX = minX;
        this.minY = minY;
        this.minZ = minZ;
        this.maxX = maxX;
        this.maxY = maxY;
        this.maxZ = maxZ;
    }

    public Cuboid(Point3D minPoint , Point3D maxPoint){
        this(minPoint.x, minPoint.y, minPoint.z, maxPoint.x, maxPoint.y, maxPoint.z);
    }

    public Cuboid(Point3D minPoint , double xLength , double yLength , double zLength){
        this(minPoint.x, minPoint.y, minPoint.z, minPoint.x + xLength, minPoint.y + yLength, minPoint.z + zLength);
    }


    /**
     * cover表示在other在内部，允许边界重合
     * @param other
     * @return
     */
    public <T extends MinimumBoundingBox>  boolean covers(T other){
        Cuboid mbb = other.getMBB();
        return (mbb.maxX<=this.maxX && mbb.minX>=this.minX)&&
                (mbb.maxY<=this.maxY && mbb.minY>=this.minY)&&
                (mbb.maxZ<=this.maxZ && mbb.minZ>=this.minZ);
    }

    /**
     * contain表示在other在内部，允许边界重合
     * 与cover相同
     * @param other
     * @return
     */
    public <T extends MinimumBoundingBox> boolean contains(T other){

        return covers(other);
    }

    /**
     * intersects包括边界重合、边界内部相交、包含关系
     * @param other
     * @return
     */
    public <T extends MinimumBoundingBox> boolean intersects(T other){
        Cuboid mbb = other.getMBB();
        return (mbb.minX<=this.maxX && mbb.maxX>=this.minX)&&
                (mbb.minY<=this.maxY && mbb.maxY>=this.minY)&&
                (mbb.minZ<=this.maxZ && mbb.maxZ>=this.minZ);
    }

    /**
     * disjoint表示边界、内部均没有相交
     * @param other
     * @return
     */
    public <T extends MinimumBoundingBox> boolean disjoint(T other){ return !intersects(other); }

    public Point3D centerPoint(){
        return new Point3D((minX+maxX)/2 , (minY+maxY)/2 , (minZ+maxZ)/2);
    }

    public Point3D minPoint() {return new Point3D(minX, minY, minZ);}

    public Optional<Cuboid> intersectedRegion(Cuboid other){
        if(!this.intersects(other)){
            return Optional.absent();
        }
        return Optional.of(new Cuboid(Math.max(minX, other.minX), Math.max(minY, other.minY), Math.max(minZ, other.minZ),
                Math.min(maxX, other.maxX),Math.min(maxY, other.maxY), Math.min(maxZ, other.maxZ) ));
    }

    public Cuboid mergedRegion(Cuboid other){
        return new Cuboid(Math.min(minX, other.minX), Math.min(minY, other.minY),Math.min(minZ, other.minZ),
                Math.max(maxX,other.maxX),Math.max(maxY,other.maxY),Math.max(maxZ,other.maxZ) );
    }

    public Cube toCube(){
        double maxSideLength = Math.max(getXSideLength(), getYSideLength());
        maxSideLength = Math.max(maxSideLength, getZSideLength());
        return new Cube(minX, minY, minZ, maxSideLength);

    }

    /**
     *向外扩张0.1
     * @param
     * @return
     */
    public Cuboid expandLittle(){
        double expand = (maxX - minX) /10000.0;
        return new Cuboid(minX - expand, minY - expand, minZ - expand , maxX + expand, maxY + expand, maxZ + expand);
    }



    public double getXSideLength(){ return maxX-minX; }

    public double getYSideLength(){
        return maxY-minY;
    }

    public double getZSideLength(){
        return maxZ-minZ;
    }

    public double getMinX() {
        return minX;
    }

    public double getMinY() {
        return minY;
    }

    public double getMinZ() {
        return minZ;
    }

    public double getMaxX() {
        return maxX;
    }

    public double getMaxY() {
        return maxY;
    }

    public double getMaxZ() {
        return maxZ;
    }



    @Override
    public Cuboid getMBB() {
        return this;
    }

    @Override
    public String toString() {
        return "Cuboid{" +
                "(" + minX +
                ", " + minY +
                "," + minZ +
                "), (" + maxX +
                ", " + maxY +
                ", " + maxZ +
                '}';
    }



}
