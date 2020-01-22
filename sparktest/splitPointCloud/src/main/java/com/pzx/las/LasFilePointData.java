package com.pzx.las;

import com.pzx.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class LasFilePointData {

    private Logger logger = Logger.getLogger(LasFilePointData.class);
    private MappedByteBuffer filePointDataBuffer;

    private byte pointDataFormatID;
    private int pointDataRecordLength;


    private long numberOfPointRecords;
    private double[] scale;
    private double[] offset;


    public LasFilePointData(MappedByteBuffer filePointDataBuffer,byte pointDataFormatID, int pointDataRecordLength, long numberOfPointRecords,double[] scale,double[] offset) {
        this.filePointDataBuffer = filePointDataBuffer;

        this.pointDataFormatID = pointDataFormatID;
        this.pointDataRecordLength = pointDataRecordLength;
        this.numberOfPointRecords = numberOfPointRecords;
        this.scale = scale;
        this.offset = offset;
    }


    /**
     * 将点数据写到内存List中
     * @return

    public List<byte[]> getPointBytesList(){


        byte[][] pointBytesArray = new byte[(int)numberOfPointRecords][];
        int index=0;
        while (filePointDataBuffer.hasRemaining()){
            //byte[] bytes = new byte[pointDataRecordLength];
            //filePointDataBuffer.get(bytes);
            pointBytesArray[index] =  resolvePointByte(readPointBytes());
            index++;
        }
        filePointDataBuffer.position(0);
        return Arrays.asList(pointBytesArray);
    }
    */

    /**
     * 将点数据写入缓冲区
     * @param pointBuffer
     */
    public void pointBytesToByteBuffer(ByteBuffer pointBuffer) {

        try {
            //byte[] bytes = new byte[pointDataRecordLength];
            long time = System.currentTimeMillis();
            while (filePointDataBuffer.hasRemaining()){
                //filePointDataBuffer.get(bytes);
                pointBuffer.put(resolvePointByte(readPointBytes()));
            }
            logger.info("-----------------------------写入缓冲区耗时"+(System.currentTimeMillis()-time));
            filePointDataBuffer.position(0);

        }catch (Exception e){
            e.printStackTrace();
        }
    }




    /**
     *从las文件中的点字节数组中，转换得到我们需要的字节数组XYZRGB（27个字节）
     * @param pointBytes 储存一个点的所有字节
     * @return 这个点的xyz坐标和rgb组成的字节数组，
     */
    private byte[] resolvePointByte(byte[] pointBytes){
        switch (pointDataFormatID){
            case 0:
                return resolvePointByteInFormat0(pointBytes);
            case 1:
                return resolvePointByteInFormat1(pointBytes);
            case 2:
                throw new RuntimeException("还没实现此类型点的读取");
            case 3:
                throw new RuntimeException("还没实现此类型点的读取");
            case 4:
                throw new RuntimeException("还没实现此类型点的读取");
            case 5:
                throw new RuntimeException("还没实现此类型点的读取");
            default:
                throw new RuntimeException("点类型读取错误");
        }

    }


    private byte[] resolvePointByteInFormat0(byte[] pointBytes){
        return resolvePointByteInFormat1(pointBytes);
    }

    /**
     * 以格式为1的方式解析点字节，将文件的中点坐标xyz转换为真实的坐标（coordinate*scale+offset）
     * las文件中以integer方式储存点坐标，转换后为Double
     * @param pointBytes
     * @return
     */
    private byte[] resolvePointByteInFormat1(byte[] pointBytes){
        byte[] bytes = new byte[27];

        double x = LittleEndianUtils.bytesToInteger(pointBytes[0],pointBytes[1],pointBytes[2],pointBytes[3])*scale[0]+offset[0];
        double y = LittleEndianUtils.bytesToInteger(pointBytes[4],pointBytes[5],pointBytes[6],pointBytes[7])*scale[1]+offset[1];
        double z = LittleEndianUtils.bytesToInteger(pointBytes[8],pointBytes[9],pointBytes[10],pointBytes[11])*scale[2]+offset[2];
        byte r = 0;
        byte g = 0;
        byte b = 0;

        byte[] xBytes = LittleEndianUtils.doubleToBytes(x);
        byte[] yBytes = LittleEndianUtils.doubleToBytes(y);
        byte[] zBytes = LittleEndianUtils.doubleToBytes(z);

        for(int i=0;i<8;i++){
            bytes[i] = xBytes[i];
        }
        for(int i=0;i<8;i++){
            bytes[8+i] = yBytes[i];
        }
        for(int i=0;i<8;i++){
            bytes[16+i] = zBytes[i];
        }
        bytes[24] = r;
        bytes[25] = g;
        bytes[26] = b;

        return bytes;
    }


    private byte[] resolvePointByteInFormat2(byte[] pointBytes){
        return null;
    }

    private byte[] resolvePointByteInFormat3(byte[] pointBytes){
        return null;
    }

    private byte[] resolvePointByteInFormat4(byte[] pointBytes){
        return null;
    }

    private byte[] resolvePointByteInFormat5(byte[] pointBytes){
        return null;
    }


    private byte[] readPointBytes(){
        return readBytes(pointDataRecordLength);
    }


    private byte[] readBytes(int size){
        byte[] bytes = new byte[size];
        filePointDataBuffer.get(bytes);
        return bytes;
    }

    public Map<String,Double> getPoint(){
        byte[] resolvedPointBytes = resolvePointByte(readPointBytes());
        Map<String,Double> pointMap = new HashMap<>();
        pointMap.put("x",LittleEndianUtils.bytesToDouble(resolvedPointBytes[0],resolvedPointBytes[1],resolvedPointBytes[2],resolvedPointBytes[3],
                resolvedPointBytes[4],resolvedPointBytes[5],resolvedPointBytes[6],resolvedPointBytes[7]));
        pointMap.put("y",LittleEndianUtils.bytesToDouble(resolvedPointBytes[8],resolvedPointBytes[9],resolvedPointBytes[10],resolvedPointBytes[11],
                resolvedPointBytes[12],resolvedPointBytes[13],resolvedPointBytes[14],resolvedPointBytes[15]));
        pointMap.put("z",LittleEndianUtils.bytesToDouble(resolvedPointBytes[16],resolvedPointBytes[17],resolvedPointBytes[18],resolvedPointBytes[19],
                resolvedPointBytes[20],resolvedPointBytes[21],resolvedPointBytes[22],resolvedPointBytes[23]));
        pointMap.put("r",(double) resolvedPointBytes[24]);
        pointMap.put("g",(double) resolvedPointBytes[25]);
        pointMap.put("b",(double) resolvedPointBytes[26]);

        return pointMap;
    }


    public long getNumberOfPointRecords() {
        return numberOfPointRecords;
    }

    @Override
    public String toString() {
        return "LasFilePointData{" +
                "filePointDataBuffer=" + filePointDataBuffer +
                ", pointDataFormatID=" + pointDataFormatID +
                ", pointDataRecordLength=" + pointDataRecordLength +
                ", numberOfPointRecords=" + numberOfPointRecords +
                ", scale=" + Arrays.toString(scale) +
                ", offset=" + Arrays.toString(offset) +
                '}';
    }
}
