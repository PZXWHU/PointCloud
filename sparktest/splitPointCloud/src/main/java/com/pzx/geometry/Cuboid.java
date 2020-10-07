package com.pzx.geometry;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.spark.sql.sources.In;
import org.checkerframework.checker.units.qual.C;

import java.io.Serializable;

public class Cuboid implements MinimumBoundingBox, Serializable {

    private double minX;
    private double minY;
    private double minZ;
    private double maxX;
    private double maxY;
    private double maxZ;

    Cuboid(){}

    public static Cuboid createFromMinAndMaxCoordinate(double minX, double minY, double minZ, double maxX, double maxY, double maxZ){
        Preconditions.checkArgument(minX<=maxX && minY<=maxY && minZ<=maxZ,
                "the first three parameters {} {} {} must be smaller than the last three {} {} {}！"
                ,minX, minY, minZ, maxX ,maxY, maxZ);
        Cuboid cuboid = new Cuboid();
        cuboid.setMinX(minX);
        cuboid.setMinY(minY);
        cuboid.setMinZ(minZ);
        cuboid.setMaxX(maxX);
        cuboid.setMaxY(maxY);
        cuboid.setMaxZ(maxZ);
        return cuboid;
    }

    public static Cuboid createFromMinPointAndSideLength(Point3D minPoint , double xLength , double yLength , double zLength){
        return createFromMinAndMaxCoordinate(minPoint.x, minPoint.y, minPoint.z,
                minPoint.x + xLength, minPoint.y + yLength, minPoint.z + zLength);
    }

    public static Cuboid createFromMinAndMaxPoint(Point3D minPoint , Point3D maxPoint){
        return createFromMinAndMaxCoordinate(minPoint.x, minPoint.y, minPoint.z, maxPoint.x, maxPoint.y, maxPoint.z);
    }

    public static Cuboid createFromCenterPointAndSideLength(Point3D centerPoint , double xLength , double yLength , double zLength){
        return createFromMinAndMaxCoordinate(centerPoint.x - xLength / 2, centerPoint.y - yLength / 2, centerPoint.z - zLength / 2,
                centerPoint.x + xLength / 2, centerPoint.y + yLength / 2, centerPoint.z + zLength / 2);
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
        return Optional.of(Cuboid.createFromMinAndMaxCoordinate(Math.max(minX, other.minX), Math.max(minY, other.minY), Math.max(minZ, other.minZ),
                Math.min(maxX, other.maxX),Math.min(maxY, other.maxY), Math.min(maxZ, other.maxZ) ));
    }

    public Cuboid mergedRegion(Cuboid other){
        return Cuboid.createFromMinAndMaxCoordinate(Math.min(minX, other.minX), Math.min(minY, other.minY),Math.min(minZ, other.minZ),
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
        return expand(expand);
    }

    public Cuboid expand(double expand){
        return Cuboid.createFromMinAndMaxCoordinate(minX - expand, minY - expand, minZ - expand,
                maxX + expand, maxY + expand, maxZ + expand);
    }

    public Cuboid expandSelf(double expand){
        this.minX = minX - expand;
        this.minY = minY - expand;
        this.minZ = minZ - expand;
        this.maxX = maxX + expand;
        this.maxY = maxY + expand;
        this.maxZ = maxZ + expand;
        return this;
    }

    public double getVolume(){
        return (maxX - minX) * (maxY - minY) * (maxZ - minZ);
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

    public void setMinX(double minX) {
        this.minX = minX;
    }

    public void setMinY(double minY) {
        this.minY = minY;
    }

    public void setMinZ(double minZ) {
        this.minZ = minZ;
    }

    public void setMaxX(double maxX) {
        this.maxX = maxX;
    }

    public void setMaxY(double maxY) {
        this.maxY = maxY;
    }

    public void setMaxZ(double maxZ) {
        this.maxZ = maxZ;
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
