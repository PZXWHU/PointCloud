package com.pzx.las;

import com.pzx.IOUtils;
import org.apache.commons.lang.ArrayUtils;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LasFilePointData {

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
     */
    public List<byte[]> getPointBytesList(){

        //List<byte[]> pointBytesList = new ArrayList<byte[]>((int)numberOfPointRecords);
        byte[][] pointBytesArray = new byte[(int)numberOfPointRecords][];
        int index=0;
        while (filePointDataBuffer.hasRemaining()){
            byte[] bytes = new byte[pointDataRecordLength];
            filePointDataBuffer.get(bytes);
            pointBytesArray[index] =  resolvePointByte(bytes);
            index++;
        }
        filePointDataBuffer.position(0);
        return Arrays.asList(pointBytesArray);
    }


    /**
     * 将点数据写入缓冲区
     * @param byteArrayOutputStream
     */
    public void pointBytesToByteArray(ByteArrayOutputStream byteArrayOutputStream) {

        /*
        if(!Files.exists(Paths.get(filePath)))
            Files.createFile(Paths.get(filePath));

        DataOutputStream outputStream = new DataOutputStream(new FileOutputStream(filePath,true));

         */
        try {
            byte[] bytes = new byte[pointDataRecordLength];
            long time = System.currentTimeMillis();
            while (filePointDataBuffer.hasRemaining()){
                filePointDataBuffer.get(bytes);
                byteArrayOutputStream.write(resolvePointByte(bytes));
            }
            filePointDataBuffer.position(0);

        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public String resolvePointByteToString(byte[] pointBytes){
        switch (pointDataFormatID){
            case 0:
                return resolvePointByteToStringInFormat0(pointBytes);
            case 1:
                return resolvePointByteToStringInFormat1(pointBytes);
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


    public String resolvePointByteToStringInFormat0(byte[] pointBytes){
        double x = LittleEndianUtils.bytesToInteger(pointBytes[0],pointBytes[1],pointBytes[2],pointBytes[3])*scale[0]+offset[0];
        double y = LittleEndianUtils.bytesToInteger(pointBytes[4],pointBytes[5],pointBytes[6],pointBytes[7])*scale[1]+offset[1];
        double z = LittleEndianUtils.bytesToInteger(pointBytes[8],pointBytes[9],pointBytes[10],pointBytes[11])*scale[2]+offset[2];
        short r = 0;
        short g = 0;
        short b = 0;
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(x).append(",").append(y).append(",").append(z).append(",").append(r).append(",").append(g).append(",").append(b).append("\n");
        return stringBuilder.toString();
    }


    public String resolvePointByteToStringInFormat1(byte[] pointBytes){
        return resolvePointByteToStringInFormat0(pointBytes);
    }


    /**
     *从las文件中的点字节数组中，转换得到我们需要的字节数组XYZRGB（27个字节）
     * @param pointBytes 储存一个点的所有字节
     * @return 这个点的xyz坐标和rgb组成的字节数组，
     */
    public byte[] resolvePointByte(byte[] pointBytes){
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


    public byte[] readPointBytes(){
        return readBytes(pointDataRecordLength);
    }


    public byte[] readBytes(int size){
        byte[] bytes = new byte[size];
        filePointDataBuffer.get(bytes);
        return bytes;
    }




}
