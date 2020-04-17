package com.pzx.las;

import com.pzx.utils.LittleEndianUtils;

import java.nio.MappedByteBuffer;

public class LasFileHeader {

    private MappedByteBuffer fileHeaderBuffer;
    private String version;

    private int headerSize;
    private long offsetToPointData;
    private long numberOfVariableLengthRecords;
    private byte pointDataFormatID;
    private int pointDataRecordLength;
    private long numberOfPointRecords;
    private double xScale;
    private double yScale;
    private double zScale;
    private double xOffset;
    private double yOffset;
    private double zOffset;
    private double maxX;
    private double maxY;
    private double maxZ;
    private double minX;
    private double minY;
    private double minZ;

    @Override
    public String toString() {
        return "LasFileHeader{" +
                "headerSize=" + headerSize +
                ", offsetToPointData=" + offsetToPointData +
                ", numberOfVariableLengthRecords=" + numberOfVariableLengthRecords +
                ", pointDataFormatID=" + pointDataFormatID +
                ", pointDataRecordLength=" + pointDataRecordLength +
                ", numberOfPointRecords=" + numberOfPointRecords +
                ", xScale=" + xScale +
                ", yScale=" + yScale +
                ", zScale=" + zScale +
                ", xOffset=" + xOffset +
                ", yOffset=" + yOffset +
                ", zOffset=" + zOffset +
                ", maxX=" + maxX +
                ", maxY=" + maxY +
                ", maxZ=" + maxZ +
                ", minX=" + minX +
                ", minY=" + minY +
                ", minZ=" + minZ +
                '}';
    }

    public LasFileHeader(MappedByteBuffer fileHeaderBuffer, String version){
        this.fileHeaderBuffer = fileHeaderBuffer;
        this.headerSize = fileHeaderBuffer.capacity();
        this.version = version;

        fileHeaderBuffer.position(96);//略过前面不需要读取的信息

        offsetToPointData = LittleEndianUtils.bytesToUnsignedInteger(readBytes(4));
        numberOfVariableLengthRecords = LittleEndianUtils.bytesToUnsignedInteger(readBytes(4));
        pointDataFormatID = fileHeaderBuffer.get();
        pointDataRecordLength = LittleEndianUtils.bytesToUnsignedShort(readBytes(2));
        numberOfPointRecords = LittleEndianUtils.bytesToUnsignedInteger(readBytes(4));


        if(Float.parseFloat(version)>=1.3)
            fileHeaderBuffer.position(fileHeaderBuffer.position()+28);// version >= 1.3 Number of points by return 28bytes
        else
            fileHeaderBuffer.position(fileHeaderBuffer.position()+20);//version <= 1.2 Number of points by return 20bytes

        xScale = Math.abs(LittleEndianUtils.bytesToDouble(readBytes(8)));
        yScale = Math.abs(LittleEndianUtils.bytesToDouble(readBytes(8)));
        zScale = Math.abs(LittleEndianUtils.bytesToDouble(readBytes(8)));

        xOffset = LittleEndianUtils.bytesToDouble(readBytes(8));
        yOffset = LittleEndianUtils.bytesToDouble(readBytes(8));
        zOffset = LittleEndianUtils.bytesToDouble(readBytes(8));

        maxX = LittleEndianUtils.bytesToDouble(readBytes(8));
        minX = LittleEndianUtils.bytesToDouble(readBytes(8));
        maxY = LittleEndianUtils.bytesToDouble(readBytes(8));
        minY = LittleEndianUtils.bytesToDouble(readBytes(8));
        maxZ = LittleEndianUtils.bytesToDouble(readBytes(8));
        minZ = LittleEndianUtils.bytesToDouble(readBytes(8));


        //System.out.println(toString());
    }

    public int getSize() {
        return headerSize;
    }

    public long getOffsetToPointData() {
        return offsetToPointData;
    }

    public long getNumberOfVariableLengthRecords() {
        return numberOfVariableLengthRecords;
    }

    public byte getPointDataFormatID() {
        return pointDataFormatID;
    }

    public int getPointDataRecordLength() {
        return pointDataRecordLength;
    }

    public long getNumberOfPointRecords() {
        return numberOfPointRecords;
    }

    public double getxScale() {
        return xScale;
    }

    public double getyScale() {
        return yScale;
    }

    public double getzScale() {
        return zScale;
    }

    public double getxOffset() {
        return xOffset;
    }

    public double getyOffset() {
        return yOffset;
    }

    public double getzOffset() {
        return zOffset;
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

    public double getMinX() {
        return minX;
    }

    public double getMinY() {
        return minY;
    }

    public double getMinZ() {
        return minZ;
    }



    public byte[] readBytes(int size){
        byte[] bytes = new byte[size];
        fileHeaderBuffer.get(bytes);
        return bytes;
    }

    public double[] getScale(){
        return new double[]{getxScale(),getyScale(),getzScale()};
    }

    public double[] getOffset(){
        return new double[]{getxOffset(),getyOffset(),getzOffset()};
    }

    /**
     *
     * @return new double[]{{maxX,maxY.maxZ,minX,minY,minZ}
     */
    public double[] getBox(){
        return new double[]{getMaxX(),getMaxY(),getMaxZ(),getMinX(),getMinY(),getMinZ()};
    }


}
