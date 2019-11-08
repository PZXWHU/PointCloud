package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class IOUtils {

    public static DataOutputStream getDataOutputStream (String resultFilePath, String resultFileName)throws IOException {
        DataOutputStream dataOutputStream = null;
        if(resultFilePath.startsWith("hdfs://")){
            Configuration configuration = new Configuration();
            FileSystem fileSystem = FileSystem.get(configuration);
            dataOutputStream = fileSystem.create(new org.apache.hadoop.fs.Path(resultFilePath+ File.separator+resultFileName),true);

        }else{
            Path nodeFilePath = Paths.get(resultFilePath,resultFileName);
            if(Files.exists(nodeFilePath))
                Files.delete(nodeFilePath);
            Files.createFile(nodeFilePath);
            FileOutputStream fileOutputStream = new FileOutputStream(nodeFilePath.toFile());
            dataOutputStream = new DataOutputStream(fileOutputStream);
        }



        return dataOutputStream;
    }


    public static void writeIntLittleEndian(OutputStream out,int v) throws IOException{
        out.write((v >>>  0) & 0xFF);
        out.write((v >>>  8) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 24) & 0xFF);
    }


}
