package com.pzx.las;

import org.apache.log4j.Logger;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class LasFile {

    private Logger logger = Logger.getLogger(LasFile.class);

    //private MappedByteBuffer fileBuffer;
    private LasFileHeader lasFileHeader;
    private LasFilePointData lasFilePointData;

    public String version;
    private byte pointDataFormatID;


    public LasFile(String filePath){

        try (FileChannel fileChannel = FileChannel.open(Paths.get(filePath))){

            long length = fileChannel.size();
            //内存映射的文件不能超过2G
            if(length>Integer.MAX_VALUE){
                logger.warn("文件大小超过2G");
                return;
            }
            //内存映射
            MappedByteBuffer fileBuffer =  fileChannel.map(FileChannel.MapMode.READ_ONLY,0,length);


            //读取数据
            version = fileBuffer.get(24)+"."+fileBuffer.get(25);

            int headerSize = LittleEndianUtils.bytesToUnsignedShort(fileBuffer.get(94),fileBuffer.get(95));
            MappedByteBuffer fileHeaderBuffer =  fileChannel.map(FileChannel.MapMode.READ_ONLY,0,headerSize);
            lasFileHeader = new LasFileHeader(fileHeaderBuffer,version);

            long offsetToPointData = lasFileHeader.getOffsetToPointData();
            long numberOfPointRecords = lasFileHeader.getNumberOfPointRecords();
            pointDataFormatID = lasFileHeader.getPointDataFormatID();
            int pointDataRecordLength = lasFileHeader.getPointDataRecordLength();

            MappedByteBuffer filePointDataBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY,offsetToPointData,numberOfPointRecords*pointDataRecordLength);
            lasFilePointData = new LasFilePointData(filePointDataBuffer,pointDataFormatID,pointDataRecordLength,numberOfPointRecords,lasFileHeader.getScale(),lasFileHeader.getOffset());


        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public LasFileHeader getLasFileHeader() {
        return lasFileHeader;
    }

    public LasFilePointData getLasFilePointData() {
        return lasFilePointData;
    }

    public String getVersion() {
        return version;
    }

    public byte getPointDataFormatID() {
        return pointDataFormatID;
    }

    public static void main(String[] args) throws IOException{
        long time = System.currentTimeMillis();
        LasFile lasFile = new LasFile("C:\\Users\\PZX\\Desktop\\新建文件夹\\spirswfst_east.las");
        System.out.println(System.currentTimeMillis()-time);
        System.out.println(lasFile.getLasFileHeader());

        /*
        time = System.currentTimeMillis();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        lasFile.getLasFilePointData().pointBytesToByteArray(byteArrayOutputStream);
        System.out.println(System.currentTimeMillis()-time);
        FileOutputStream fileWriter = new FileOutputStream("C:\\Users\\PZX\\Desktop\\新建文件夹\\tmp.tmp");
        fileWriter.write(byteArrayOutputStream.toByteArray());
        fileWriter.close();
        System.out.println(System.currentTimeMillis()-time);



        time = System.currentTimeMillis();
        List<byte[]> list = lasFile.getLasFilePointData().getPointBytesList();
        System.out.println(System.currentTimeMillis()-time);

         */
    }


}
