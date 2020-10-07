package com.pzx.geometry;

import com.pzx.utils.SplitUtils;
import org.apache.commons.lang.ArrayUtils;

import java.beans.beancontext.BeanContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.Buffer;
import java.util.Arrays;

public class Point3D implements MinimumBoundingBox, Serializable {

    public final double x;
    public final double y;
    public final double z;
    public final byte r;
    public final byte g;
    public final byte b;
    private double[] normalVector;
    private boolean isLocalPartition;



    public Point3D(double x , double y , double z){
        this.x = x;
        this.y = y;
        this.z = z;
        this.r = 0;
        this.g = 0;
        this.b = 0;
        normalVector = null;
        isLocalPartition = true;

    }

    public Point3D(double x , double y , double z, byte r, byte g, byte b){
        this.x = x;
        this.y = y;
        this.z = z;
        this.r = r;
        this.g = g;
        this.b = b;
        normalVector = null;
        isLocalPartition = true;

    }

    public double[] getNormalVector() {
        return normalVector;
    }

    public void setNormalVector(double[] normalVector) {
        this.normalVector = normalVector;
    }

    public void setIsLocalPartition(boolean localPartition) {
        isLocalPartition = localPartition;
    }

    public boolean isLocalPartition() {
        return isLocalPartition;
    }

    public double distance(Point3D point3D){
        return Math.sqrt(Math.pow((point3D.x - x),2) + Math.pow((point3D.y - y),2) + Math.pow((point3D.z - z),2));
    }

    public byte[] serialize(double[] xyzOffset, double scale[]){
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            int newX = (int)((x-xyzOffset[0])/scale[0]);
            int newY = (int)((y-xyzOffset[1])/scale[1]);
            int newZ = (int)((z-xyzOffset[2])/scale[2]);
            byte[] coordinateBytes = SplitUtils.pointInZigZagFormat(new int[]{newX,newY,newZ});
            byte[] colorBytes = new byte[]{r,g,b};
            byteArrayOutputStream.write(coordinateBytes);
            byteArrayOutputStream.write(colorBytes);
            if (normalVector != null){
                int normalX = (int)(normalVector[0] / scale[0]);
                int normalY = (int)(normalVector[1] / scale[1]);
                int normalZ = (int)(normalVector[2] / scale[2]);
                byte[] normalVectorBytes = SplitUtils.pointInZigZagFormat(new int[]{normalX, normalY, normalZ});
                byteArrayOutputStream.write(normalVectorBytes);
            }
        }catch (IOException e){
            throw new RuntimeException(e.toString());
        }

        return byteArrayOutputStream.toByteArray();
    }

    public Point3D copy(){
        Point3D point3D = new Point3D(x,y,z,r,g,b);
        point3D.setNormalVector(normalVector);
        point3D.setIsLocalPartition(isLocalPartition);
        return point3D;
    }

    @Override
    public Cuboid getMBB() {
        return Cuboid.createFromMinAndMaxCoordinate(x,y,z,x,y,z);
    }

    @Override
    public String toString() {
        return "Point3D{" +
                "x=" + x +
                ", y=" + y +
                ", z=" + z +
                ", r=" + r +
                ", g=" + g +
                ", b=" + b +
                ", normalVector=" + Arrays.toString(normalVector) +
                ", isLocalPartition=" + isLocalPartition +
                '}';
    }
}
