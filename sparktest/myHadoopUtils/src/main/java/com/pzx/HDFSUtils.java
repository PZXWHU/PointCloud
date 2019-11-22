package com.pzx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class HDFSUtils {

    private static Logger logger = Logger.getLogger(HDFSUtils.class);
    private static FileSystem hdfsFs = init();


    private static FileSystem init(){
        try {
            Configuration conf = new Configuration();

            InputStream hdfsPropertiesInputStream = HDFSUtils.class.getClassLoader().getResourceAsStream("hdfs.conf");
            Properties hdfsProperties = new Properties();
            hdfsProperties.load(hdfsPropertiesInputStream);
            for(String hdfsPropertyNames:hdfsProperties.stringPropertyNames()){
                conf.set(hdfsPropertyNames,hdfsProperties.getProperty(hdfsPropertyNames));
            }

            FileSystem hdfsFs = FileSystem.get(conf);
            return hdfsFs;
        }catch (IOException e){
            e.printStackTrace();
            return null;
        }

    }


    /**
     * 返回新创建的FileSystem，避免类中静态变量hdfsFs被关闭
     * @return
     */
    public static FileSystem getFileSystem(){
        //FileSystem fileSystem = HDFSUtils.init();
        return HDFSUtils.init();
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
        FileSystem hdfsFs = getFileSystem();
        FSDataInputStream inputStream = hdfsFs.open(new Path(filePath));
        int length = inputStream.available();
        byte[] bytes = new byte[length];
        inputStream.readFully(0,bytes);
        inputStream.close();

        return bytes;


    }

    public static void deleteFile(String filePath)throws IOException{

        FileSystem hdfsFs = getFileSystem();
        hdfsFs.deleteOnExit(new Path(filePath));


    }


    public static List<String> listFile(String path){
        return listFileIsOrNotRecursive(path,false);
    }

    public static List<String> listFileRecursive(String path){
        return listFileIsOrNotRecursive(path,true);
    }


    private static List<String> listFileIsOrNotRecursive(String path,boolean recursive){
        FileSystem hdfsFs = getFileSystem();
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

    }



    public static void main(String[] args){
        init();
        List<String> list = listFile("hdfs://master:9000/pzx/pysparkresult");
        for(String path :list){
            System.out.println(path);
        }
    }

}
