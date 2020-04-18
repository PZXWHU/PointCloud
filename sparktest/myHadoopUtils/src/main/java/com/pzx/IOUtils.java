package com.pzx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class IOUtils {


    private static Logger logger = Logger.getLogger(IOUtils.class);



    public static DataOutputStream getDataOutputStream (String outputFilePath,boolean append)throws IOException{

        DataOutputStream dataOutputStream = null;

        if(outputFilePath.startsWith("hdfs://")){

            FileSystem fileSystem = HDFSUtils.getFileSystem();

            if (append&&fileSystem.exists(new org.apache.hadoop.fs.Path(outputFilePath))){
                //追加模式且hdfs已经存在此文件
                dataOutputStream = fileSystem.append(new org.apache.hadoop.fs.Path(outputFilePath));
            }else if(!fileSystem.exists(new org.apache.hadoop.fs.Path(outputFilePath))) {
                //hdfs不存在此文件
                dataOutputStream = fileSystem.create(new org.apache.hadoop.fs.Path(outputFilePath));
            }else {
                //hdfs不存在此文件且追加模式
                dataOutputStream = fileSystem.create(new org.apache.hadoop.fs.Path(outputFilePath),true);
            }

        }else{
            FileOutputStream fileOutputStream = new FileOutputStream(outputFilePath,append);
            dataOutputStream = new DataOutputStream(fileOutputStream);
        }

        return dataOutputStream;
    }



    public static void writerDataToFile(String outputFilePath,byte[] data,boolean append){

        try {
            DataOutputStream dataOutputStream = getDataOutputStream(outputFilePath,append);
            dataOutputStream.write(data);
            dataOutputStream.close();
        }catch (IOException e){
            throw new RuntimeException("写文件失败！");
        }

    }


    public static void writerDataToFile(String outputFilePath,byte[][] dataArray,boolean append){

        try {
            DataOutputStream dataOutputStream = getDataOutputStream(outputFilePath,append);
            for(byte[] data : dataArray){
                dataOutputStream.write(data);
            }
            dataOutputStream.close();
        }catch (IOException e){
            e.printStackTrace();
            throw new RuntimeException("写文件失败！");
        }
    }

    public static void writerDataToFile(String outputFilePath, Iterator<byte[]> dataIterator, boolean append){

        try {
            DataOutputStream dataOutputStream = getDataOutputStream(outputFilePath,append);
            while (dataIterator.hasNext()){
                dataOutputStream.write(dataIterator.next());
            }
            dataOutputStream.close();
        }catch (IOException e){
            e.printStackTrace();
            throw new RuntimeException("写文件失败！");
        }
    }


    public static void writeIntLittleEndian(OutputStream outputStream,int i){
        try {
            outputStream.write(i&0xff);
            outputStream.write((i>>8)&0xff);
            outputStream.write((i>>16)&0xff);
            outputStream.write((i>>24)&0xff);
        }catch (Exception e){
            e.printStackTrace();
        }

    }



    public static List<String> listAllFiles(String dataDirStr){

        if(dataDirStr.startsWith("hdfs://")){
            List<String> list = HDFSUtils.listFileRecursive(dataDirStr);
            return list;
        }else {
            try {
                List<String> list = new ArrayList<>();
                List<Path> pathList = Files.list(Paths.get(dataDirStr)).collect(Collectors.toList());
                for(Path path : pathList){
                    if(Files.isDirectory(path)){
                        list.addAll(listAllFiles(path.toString()));
                    }else
                        list.add(path.toString());
                }
                return list;
            }catch (IOException e){
                e.printStackTrace();

                return null;
            }

        }

    }


}
