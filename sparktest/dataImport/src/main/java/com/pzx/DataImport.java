package com.pzx;

import org.apache.hadoop.hbase.TableName;
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
                return;
            }
            String tableName = args[0];
            String dataDirStr = args[1];
            HBaseUtils.init();

            if(!HBaseUtils.getAdmin().tableExists(TableName.valueOf(tableName))){
                HBaseUtils.createTable(tableName,new String[]{"data"});
            }


            Table table = HBaseUtils.connection.getTable(TableName.valueOf(tableName));
            file2HBase(dataDirStr,table);

        }catch (Exception e){
            logger.warn("failed");
            e.printStackTrace();
        }finally {
            HBaseUtils.close();
        }
    }


    public static void file2HBase(String dataDirStr,Table table)throws IOException {
        List<Put> puts = new ArrayList<>();

        List<String> filePathList = IOUtils.listAllFiles(dataDirStr);
        int count = 0;
        for (String filePath : filePathList) {

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

            count++;
            if (count >= 50) {
                logger.info("开始插入！");
                table.put(puts);
                puts.clear();
                count = 0;
                logger.info("插入完成！");

            }
        }
        logger.info("开始插入！");
        table.put(puts);
        puts.clear();
        logger.info("插入完成！");



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
