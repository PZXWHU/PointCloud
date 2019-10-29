package HBaseAPI.MapReduceAPI;

import org.apache.commons.cli.*;
import org.apache.commons.codec.digest.DigestUtils;


import org.apache.commons.io.input.SwappedDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.*;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;

import java.io.InputStreamReader;
import java.util.Properties;

public class ImportFile2HBase {

    public enum Counters{LINES};


    static class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private byte[] family;
        private byte[] qualifier;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            System.out.println("setup!");
            String column = context.getConfiguration().get("conf.column");
            byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes(column));
            family = colkey[0];
            if (colkey.length>1)
                qualifier = colkey[1];
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String line = value.toString();
            byte[] rowkey = DigestUtils.md5(line);

            Put put = new Put(rowkey);
            put.addColumn(family,qualifier,line.getBytes());
            context.write(new ImmutableBytesWritable(rowkey),put);
            context.getCounter(Counters.LINES).increment(1);
        }
    }

    private static CommandLine parseArgs(String[] args) throws ParseException{

        Options options = new Options();
        Option o = new Option("t","table",true,"table to import into");
        o.setArgName("table-name");
        o.setRequired(true);
        options.addOption(o);

        o = new Option("c","column",true,"column to store row data");
        o.setArgName("family:qualifier");
        o.setRequired(true);
        options.addOption(o);

        o = new Option("i","input",true,"the dir or file to read from");
        o.setArgName("path in HDFS");
        o.setRequired(true);
        options.addOption(o);

        options.addOption("d","debug",false,"suitch  on debug log level");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd =parser.parse(options,args);


        }catch (Exception e){

        }

        return  cmd;


    }

    public static void main(String[] args) {
        try {
            //Properties properties = System.getProperties();
            //properties.setProperty("HADOOP_USER_NAME", "root");

            Configuration configuration = null;
            configuration = HBaseConfiguration.create();

            configuration.set("hbase.rootdir", "hdfs://master:8020/HBase_DB");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");



            /*
            configuration.set("yarn.resourcemanager.hostname", "master");
            configuration.set("mapreduce.framework.name", "yarn");
            configuration.set("fs.defaultFS", "hdfs://master:8020/");


            configuration.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            configuration.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
            configuration.set("mapreduce.app-submission.cross-platform", "true");// Windows开发者需要设置跨平台
            */

            /*
            configuration.set("yarn.application.classpath",
                    "/usr/local/hadoop/etc/hadoop,"
                            +"/usr/local/hadoop/share/hadoop/common/*,"
                            +"/usr/local/hadoop/share/hadoop/common/lib/*,"
                            +"/usr/local/hadoop/share/hadoop/hdfs/*,"
                            +"/usr/local/hadoop/share/hadoop/hdfs/lib/*,"
                            +"/usr/local/hadoop/share/hadoop/mapreduce/*,"
                            +"/usr/local/hadoop/share/hadoop/mapreduce/lib/*,"
                            +"/usr/local/hadoop/share/hadoop/yarn/*,"
                            +"/usr/local/hadoop/share/hadoop/yarn/lib/*,"
                            +"/usr/local/hbase/lib/*");
            */


            /*
                String [] otherArgs = new GenericOptionsParser(configuration,args).getRemainingArgs();
                CommandLine cmd = parseArgs(otherArgs);
                String table = cmd.getOptionValue("t");
                String input = cmd.getOptionValue("i");
                String column = cmd.getOptionValue("c");
            */

            String table = "myFile";
            String input = "hdfs://master:8020/pzxFile";
            String column = "data:line";

            configuration.set("conf.column",column);

            Job job = Job.getInstance(configuration,"input file form file "+input+" into table"+table);

            job.setJarByClass(ImportFile2HBase.class);

            job.setMapperClass(ImportMapper.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TableOutputFormat.class);

            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);

            FileInputFormat.addInputPath(job,new Path(input));
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,table);

            job.setNumReduceTasks(0);

            Boolean b = job.waitForCompletion(true);

            if(!b)
                System.out.println("job failed!");


        }catch (Exception e){
            e.printStackTrace();
        }



    }





}
