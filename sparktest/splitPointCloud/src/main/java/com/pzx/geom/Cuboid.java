package com.pzx.geom;


import com.google.common.base.Preconditions;

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
                "the first three parameters must be smaller than the last three！");
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


    /**
     * 将cuboid分为八个子cuboid
     * @return
     */
    public Cuboid[] split(){
        /*
        Cuboid[] childrenCuboid = new Cuboid[8];

        Point3D centerPoint3D = centerPoint();
        double midX = centerPoint3D.x;
        double midY = centerPoint3D.y;
        double midZ = centerPoint3D.z;

        double xLength = (maxX - minX) / 2;
        double yLength = (maxY - minY) / 2;
        double zLength = (maxZ - minZ) / 2;

        childrenCuboid[REGION_SX_SY_SZ] = new Cuboid(new Point3D(minX,minY,minZ),xLength,yLength,zLength);
        childrenCuboid[REGION_BX_SY_SZ] = new Cuboid(new Point3D(midX,minY,minZ),xLength,yLength,zLength);
        childrenCuboid[REGION_SX_BY_SZ] = new Cuboid(new Point3D(minX,midY,minZ),xLength,yLength,zLength);
        childrenCuboid[REGION_BX_BY_SZ] = new Cuboid(new Point3D(midX,midY,minZ),xLength,yLength,zLength);

        childrenCuboid[REGION_SX_SY_BZ] = new Cuboid(new Point3D(minX,minY,midZ),xLength,yLength,zLength);
        childrenCuboid[REGION_BX_SY_BZ] = new Cuboid(new Point3D(midX,minY,midZ),xLength,yLength,zLength);
        childrenCuboid[REGION_SX_BY_BZ] = new Cuboid(new Point3D(minX,midY,midZ),xLength,yLength,zLength);
        childrenCuboid[REGION_BX_BY_BZ] = new Cuboid(new Point3D(midX,midY,midZ),xLength,yLength,zLength);

         */

        double childXLength = (maxX - minX) / 2;
        double childYLength = (maxY - minY) / 2;
        double childZLength = (maxZ - minZ) / 2;

        Cuboid[] childrenCuboid = new Cuboid[8];
        for(int i=0 ; i<=7 ; i ++){
            double newMinX = minX + childXLength * (i>>2 & 1);
            double newMinY = minY + childYLength * (i>>1 & 1);
            double newMinZ = minZ + childZLength * (i>>0 & 1);
            childrenCuboid[i] = new Cuboid(newMinX,newMinY,newMinZ,
                    newMinX+childXLength,newMinY+childYLength,newMinZ+childZLength);
        }

        return childrenCuboid;
    }


    public static void main(String[] args) {
        Cuboid cuboid = new Cuboid(0,0,0,50,50,50);
        Cuboid cuboid1 = new Cuboid(2,2,2,49,49,49);
        System.out.println(cuboid.intersects(cuboid1));
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
