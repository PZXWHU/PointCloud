package com.pzx.las;

import org.apache.log4j.Logger;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LasFile {

    private Logger logger = Logger.getLogger(LasFile.class);

    //private MappedByteBuffer fileBuffer;
    private LasFileHeader lasFileHeader;
    private List<LasFilePointData> lasFilePointDataList = new ArrayList<>();

    public String version;
    private byte pointDataFormatID;


    @Override
    public String toString() {
        return "LasFile{" +
                "lasFileHeader=" + lasFileHeader +
                ", lasFilePointDataList=" + lasFilePointDataList +
                ", version='" + version + '\'' +
                ", pointDataFormatID=" + pointDataFormatID +
                '}';
    }

    public LasFile(String filePath){

        try (FileChannel fileChannel = FileChannel.open(Paths.get(filePath))){


            MappedByteBuffer tmpBuffer =  fileChannel.map(FileChannel.MapMode.READ_ONLY,0,100);
            //读取数据
            version = tmpBuffer.get(24)+"."+tmpBuffer.get(25);

            int headerSize = LittleEndianUtils.bytesToUnsignedShort(tmpBuffer.get(94),tmpBuffer.get(95));
            MappedByteBuffer fileHeaderBuffer =  fileChannel.map(FileChannel.MapMode.READ_ONLY,0,headerSize);
            lasFileHeader = new LasFileHeader(fileHeaderBuffer,version);

            long offsetToPointData = lasFileHeader.getOffsetToPointData();
            long numberOfPointRecords = lasFileHeader.getNumberOfPointRecords();
            pointDataFormatID = lasFileHeader.getPointDataFormatID();
            int pointDataRecordLength = lasFileHeader.getPointDataRecordLength();

            long pointDataBytesCount = numberOfPointRecords*pointDataRecordLength;
            int pointBytesPerBuffer = (Integer.MAX_VALUE/pointDataRecordLength)*pointDataRecordLength;
            while (pointDataBytesCount>0){
                long pointBytesThisBuffer = Math.min(pointBytesPerBuffer,pointDataBytesCount);

                MappedByteBuffer filePointDataBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY,offsetToPointData,pointBytesThisBuffer);
                LasFilePointData lasFilePointData = new LasFilePointData(filePointDataBuffer,pointDataFormatID,pointDataRecordLength,pointBytesThisBuffer/pointDataRecordLength,lasFileHeader.getScale(),lasFileHeader.getOffset());
                lasFilePointDataList.add(lasFilePointData);

                pointDataBytesCount -=pointBytesPerBuffer;
                offsetToPointData += pointBytesPerBuffer;
            }



            /*
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


             */

        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public LasFileHeader getLasFileHeader() {
        return lasFileHeader;
    }

    public List<LasFilePointData> getLasFilePointDataList() {
        return lasFilePointDataList;
    }

    public String getVersion() {
        return version;
    }

    public byte getPointDataFormatID() {
        return pointDataFormatID;
    }

    public static void main(String[] args) throws IOException{
        long time = System.currentTimeMillis();
        LasFile lasFile = new LasFile("D:\\wokspace\\点云的储存与可视化\\大数据集与工具\\data\\hn3\\C_51DN2.LAZ");
        System.out.println(System.currentTimeMillis()-time);
        System.out.println(lasFile.getLasFileHeader().getNumberOfPointRecords());


    }


}
