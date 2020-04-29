package com.pzx;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;

public class BulkLoad {

    public static void main(String[] args) throws Exception {

        if(args.length !=2){
            System.out.println("请输入文件目录和表名");
            return;
        }

        String filesDir = args[0];
        SparkSession spark = SparkSession.builder().appName("bulkLoad").master("local").getOrCreate();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

        JavaPairRDD<String,String> unresolvedRDD = javaSparkContext.wholeTextFiles(filesDir);
        JavaPairRDD<String,byte[]> pointCloudRDD = unresolvedRDD.mapToPair(tuple->{
            String filePath = tuple._1;
            String fileNameWithSuffix = filePath.substring(filePath.lastIndexOf(File.separator)+1);

            return new Tuple2<String, byte[]>(fileNameWithSuffix, tuple._2.getBytes(Charsets.ISO_8859_1));
        });

        JavaPairRDD<ImmutableBytesWritable, KeyValue> hFileRDD = pointCloudRDD.mapToPair(tuple->{
            String fileNameWithSuffix = tuple._1;
            String fileName = fileNameWithSuffix.split("\\.")[0];
            String suffix = fileNameWithSuffix.split("\\.")[1];

            byte[] rowKey = Bytes.toBytes(fileName);
            byte[] columnFamily = Bytes.toBytes("data");
            byte[] columnQualifier = Bytes.toBytes(suffix);
            ImmutableBytesWritable immutableRowKey = new ImmutableBytesWritable(rowKey);
            KeyValue keyValue = new KeyValue(rowKey, columnFamily, columnQualifier, tuple._2);
            return new Tuple2<ImmutableBytesWritable, KeyValue>(immutableRowKey, keyValue);

        });

        String tableName = args[1];
        bulkLoad(hFileRDD, tableName);

    }


    public static void bulkLoad(JavaPairRDD<String,byte[]> rdd, String keySeparator, String tableName) throws Exception{

        JavaPairRDD<ImmutableBytesWritable, KeyValue> hFileRDD = rdd.mapToPair(tuple->{

            String[] keyArray = tuple._1.split(keySeparator);
            byte[] rowKey = Bytes.toBytes(keyArray[0]);
            byte[] columnFamily = Bytes.toBytes(keyArray[1]);
            byte[] columnQualifier = Bytes.toBytes(keyArray[2]);
            ImmutableBytesWritable immutableRowKey = new ImmutableBytesWritable(rowKey);
            KeyValue keyValue = new KeyValue(rowKey, columnFamily, columnQualifier, tuple._2);
            return new Tuple2<ImmutableBytesWritable, KeyValue>(immutableRowKey, keyValue);
        });
        bulkLoad(hFileRDD, tableName);

    }



    public static void bulkLoad(JavaPairRDD<ImmutableBytesWritable, KeyValue> hFileRDD, String tableName) throws Exception{

        Configuration hConf = HBaseUtils.getConfiguration();

        HBaseUtils.createTable(tableName, new String[]{"data"});
        hConf.set("hbase.mapreduce.hfileoutputformat.table.name", tableName);
        hConf.set("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", "3200");

        String hFileOutPut = "hdfs://master:9000/pzx/hFile";

        hFileRDD.saveAsNewAPIHadoopFile(hFileOutPut, ImmutableBytesWritable.class, KeyValue.class, HFileOutputFormat2.class, hConf);
        bulkLoad(hFileOutPut, tableName, hConf);

    }

    public static void bulkLoad(String hFileOutPut, String tableName, Configuration hConf) throws Exception{

        TableName hTableName = TableName.valueOf(tableName);
        Connection connection = ConnectionFactory.createConnection(hConf);
        Table table = connection.getTable(hTableName);
        RegionLocator regionLocator = connection.getRegionLocator(hTableName);

        LoadIncrementalHFiles bulkLoader = new LoadIncrementalHFiles(hConf);
        bulkLoader.doBulkLoad(new Path(hFileOutPut), connection.getAdmin(), table, regionLocator);
        HDFSUtils.deleteFile(hFileOutPut, true);

    }

}
