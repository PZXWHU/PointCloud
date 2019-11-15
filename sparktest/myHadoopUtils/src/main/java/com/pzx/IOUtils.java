package com.pzx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class IOUtils {


    private static Logger logger = Logger.getLogger(IOUtils.class);

/*
    public static DataOutputStream getDataOutputStream (String outputFilePath,boolean append)throws IOException {
        DataOutputStream dataOutputStream = null;
        if(outputFilePath.startsWith("hdfs://")){

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://master:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fileSystem = FileSystem.get(conf);

            if (append&&fileSystem.exists(new org.apache.hadoop.fs.Path(outputFilePath))){
                dataOutputStream = fileSystem.append(new org.apache.hadoop.fs.Path(outputFilePath));
            }else if(!fileSystem.exists(new org.apache.hadoop.fs.Path(outputFilePath))) {
                dataOutputStream = fileSystem.create(new org.apache.hadoop.fs.Path(outputFilePath));
            }else {
                dataOutputStream = fileSystem.create(new org.apache.hadoop.fs.Path(outputFilePath),true);
            }
            return dataOutputStream;


        }else{
            FileOutputStream fileOutputStream = new FileOutputStream(outputFilePath,append);
            dataOutputStream = new DataOutputStream(fileOutputStream);
        }

        return dataOutputStream;
    }


 */



    public static void writerDataToFile(String outputFilePath,byte[] data,boolean append){

        try {
            DataOutputStream dataOutputStream = null;
            if(outputFilePath.startsWith("hdfs://")){

                FileSystem fileSystem = HDFSUtils.getFileSystem();

                if (append&&fileSystem.exists(new org.apache.hadoop.fs.Path(outputFilePath))){
                    dataOutputStream = fileSystem.append(new org.apache.hadoop.fs.Path(outputFilePath));
                }else if(!fileSystem.exists(new org.apache.hadoop.fs.Path(outputFilePath))) {
                    dataOutputStream = fileSystem.create(new org.apache.hadoop.fs.Path(outputFilePath));
                }else {
                    dataOutputStream = fileSystem.create(new org.apache.hadoop.fs.Path(outputFilePath),true);
                }

                dataOutputStream.write(data);
                dataOutputStream.close();



            }else{
                FileOutputStream fileOutputStream = new FileOutputStream(outputFilePath,append);
                dataOutputStream = new DataOutputStream(fileOutputStream);
                dataOutputStream.write(data);
                dataOutputStream.close();

            }
        }catch (IOException e){
            e.printStackTrace();
            logger.warn("数据写入文件失败！");
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
