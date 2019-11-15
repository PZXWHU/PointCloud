package com.pzx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HDFSUtils {

    private static Logger logger = Logger.getLogger(HDFSUtils.class);
    //private static FileSystem hdfsFs = null;


    public static FileSystem init(){
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://master:9000");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem hdfsFs = FileSystem.get(conf);
            return hdfsFs;
        }catch (IOException e){
            e.printStackTrace();
            return null;
        }

        /*
        DataInputStream getIt = fs.open(new Path("hdfs://master:8020/pzxFile/file.txt"));
        BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
        String content = d.readLine(); //读取文件一行
        System.out.println(content);
        fs.close();

         */
    }


    public static FileSystem getFileSystem(){
        FileSystem fileSystem = HDFSUtils.init();
        return fileSystem;
    }

    public static void write(){

    }

    public static void close(FileSystem hdfsFs){
        try {
            if(hdfsFs!=null)
                hdfsFs.close();
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    public static byte[] read(String filePath) throws IOException{
        FileSystem hdfsFs = init();
            FSDataInputStream inputStream = hdfsFs.open(new Path(filePath));
            int length = inputStream.available();
            byte[] bytes = new byte[length];
            inputStream.readFully(0,bytes);
            inputStream.close();
         close(hdfsFs);
            return bytes;


    }

    public static void deleteFile(String filePath)throws IOException{

        FileSystem hdfsFs = init();
        hdfsFs.deleteOnExit(new Path(filePath));
        close(hdfsFs);

    }


    public static List<String> listFile(String path){
        return listFileIsOrNotRecursive(path,false);
    }

    public static List<String> listFileRecursive(String path){
        return listFileIsOrNotRecursive(path,true);
    }


    private static List<String> listFileIsOrNotRecursive(String path,boolean recursive){
        FileSystem hdfsFs = init();
        List<String> fileList = new ArrayList<>();
        try {
            RemoteIterator<LocatedFileStatus> iterator = hdfsFs.listFiles(new Path(path),recursive);
            while (iterator.hasNext()){
                LocatedFileStatus  locatedFileStatus = iterator.next();
                fileList.add(locatedFileStatus.getPath().toString());
            }
            return fileList;
        }catch (IOException e){
            e.printStackTrace();
            logger.warn("HDFS文件列出失败");
            return null;
        }
        finally {
            close(hdfsFs);
        }
    }



    public static void main(String[] args){
        init();
        List<String> list = listFile("hdfs://master:9000/pzx/pysparkresult");
        for(String path :list){
            System.out.println(path);
        }
    }

}
