package com.pzx;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import java.util.stream.Collectors;

/**
 * Hello world!
 *
 */
public class DataImport
{
    private static Logger logger = Logger.getLogger(DataImport.class);

    public static void main( String[] args )
    {
        try{

            if(args.length!=2){
                System.out.println("请指定表名和数据文件夹名！");
                logger.info("请指定表名和数据文件夹名！");
                return;
            }
            String tableName = args[0];
            String dataDirStr = args[1];

            Connection hbaseConnection = HBaseUtils.getConnection();
            HBaseUtils.createTable(tableName,new String[]{"data"});

            file2HBase(dataDirStr,tableName,hbaseConnection);

        }catch (Exception e){
            logger.warn("failed");
            e.printStackTrace();
        }
    }


    public static void file2HBase(String dataDirStr,String tableName, Connection hbaseConnection)throws Exception {

        Table table = hbaseConnection.getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<>();

        List<String> filePathList = IOUtils.listAllFiles(dataDirStr);

        int insertedFileNum = 0;

        for (int i=0;i<filePathList.size();i++) {
            String filePath = filePathList.get(i);
            String suffix = filePath.split("\\.")[filePath.split("\\.").length - 1];
            switch (suffix) {
                case "hrc":
                    puts.add(createPutFormFile(filePath, "data", "hrc"));
                    break;
                case "bin":
                    puts.add(createPutFormFile(filePath, "data", "bin"));
                    break;
                case "las":
                    puts.add(createPutFormFile(filePath, "data", "las"));
                    break;
                case "js":
                    puts.add(createPutFormFile(filePath, "data", "js"));
                    break;
                default:
                    break;
            }




            if (puts.size() >= 100||i==filePathList.size()-1) {

                table.put(puts);
                insertedFileNum += puts.size();
                puts.clear();
                logger.info("已完成文件插入：" + insertedFileNum + ",上一个插入文件为：" + filePathList.get(insertedFileNum-1));
                Thread.sleep(1000);
            }
        }

    }





    public static Put createPutFormFile(String filePath,String colFamily,String colName){

        try {
            Put put;
            if(filePath.startsWith("hdfs://")){
                put = new Put(Bytes.toBytes(filePath.substring(filePath.lastIndexOf("/")+1,filePath.lastIndexOf("."))));
                put.addColumn(colFamily.getBytes(), colName.getBytes(), HDFSUtils.read(filePath));
            }else {
                byte[] bytes = Files.readAllBytes(Paths.get(filePath));
                put = new Put(Bytes.toBytes(filePath.substring(filePath.lastIndexOf(File.separator)+1,filePath.lastIndexOf("."))));
                put.addColumn(colFamily.getBytes(), colName.getBytes(), bytes);
            }
            return put;
        }catch (IOException e){
            e.printStackTrace();
            logger.info("读取文件异常");
            return null;
        }


    }


}
