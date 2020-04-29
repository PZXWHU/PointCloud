package com.pzx.geometry;






import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.checkerframework.checker.units.qual.C;
import org.json4s.scalap.scalasig.ThisType;

import javax.validation.constraints.Max;
import java.io.Serializable;

public class Cuboid implements WithCuboidMBR, Serializable {

    private double minX;
    private double minY;
    private double minZ;
    private double maxX;
    private double maxY;
    private double maxZ;


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
    public boolean covers(Cuboid other){
        return (other.maxX<=this.maxX && other.minX>=this.minX)&&
                (other.maxY<=this.maxY && other.minY>=this.minY)&&
                (other.maxZ<=this.maxZ && other.minZ>=this.minZ);
    }

    /**
     * contains包括与边界重合的点
     * @param point3D
     * @return
     */
    public boolean contains(Point3D point3D){
        return contains(point3D.getCuboidMBR());
    }

    /**
     * contain表示在other在内部，允许边界重合
     * 与cover相同
     * @param other
     * @return
     */
    public boolean contains(Cuboid other){
        return covers(other);
    }

    public <T extends WithCuboidMBR> boolean contains(T other){
        return contains(other.getCuboidMBR());
    }


    /**
     * intersects包括边界重合、边界内部相交、包含关系
     * @param other
     * @return
     */
    public boolean intersects(Cuboid other){

        return (other.minX<=this.maxX && other.maxX>=this.minX)&&
                (other.minY<=this.maxY && other.maxY>=this.minY)&&
                (other.minZ<=this.maxZ && other.maxZ>=this.minZ);
    }

    public <T extends WithCuboidMBR> boolean intersects(T other){
        return intersects(other.getCuboidMBR());
    }

    /**
     * disjoint表示边界、内部均没有相交
     * @param other
     * @return
     */
    public boolean disjoint(Cuboid other){
        return !intersects(other);
    }

    public <T extends WithCuboidMBR> boolean disjoint(T other){
        return disjoint(other.getCuboidMBR());
    }

    public Point3D centerPoint(){
        return new Point3D((minX+maxX)/2 , (minY+maxY)/2 , (minZ+maxZ)/2);
    }

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

    public Cube toBoundingBoxCube(){
        double maxSidelength = Math.max(getXSideLength(), getYSideLength());
        maxSidelength = Math.max(maxSidelength, getZSideLength());
        return new Cube(minX, minY, minZ, maxSidelength);

    }

    /**
     * 将cuboid分为八个子cuboid
     * @return
     */
    public Cuboid[] split(){

        double childXLength = (maxX - minX) / 2;
        double childYLength = (maxY - minY) / 2;
        double childZLength = (maxZ - minZ) / 2;

        Cuboid[] childrenCuboid = new Cuboid[8];
        for(int i=0 ; i<=7 ; i ++){
            double newMinX = minX + childXLength * (i>>2 & 1);
            double newMinY = minY + childYLength * (i>>1 & 1);
            double newMinZ = minZ + childZLength * (i>>0 & 1);
/*
            if(childXLength<0){
                System.out.println(childXLength);
                System.out.println(this);
            }

 */
            childrenCuboid[i] = new Cuboid(newMinX,newMinY,newMinZ,
                    newMinX+childXLength,newMinY+childYLength,newMinZ+childZLength);
        }

        return childrenCuboid;
    }

    /**
     *向外扩张0.1
     * @param
     * @return
     */
    public Cuboid expandLittle(){
        double expand = (maxX - minX) /10000.0;
        minX = minX - expand;
        minY = minY - expand;
        minZ = minZ - expand;
        maxX = maxX + expand;
        maxY = maxY + expand;
        maxZ = maxZ + expand;
        return this;
    }

    public double[] getBoundingBox(){return new double[]{maxX, maxY, maxZ, minX, minY, minZ};}

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
    public Cuboid getCuboidMBR() {
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
