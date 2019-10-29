package HBaseAPI.MapReduceAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class WordCount {

    public  static class WordMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);

        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }

        }
    }


    public  static class WordReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable() ;

        public void reduce(Text key , Iterable<IntWritable> values, Context context) throws IOException , InterruptedException {
            int sum = 0 ;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }


    public static void main(String[] args) throws Exception{

        /*通过连接hbase执行mapreduce
        Configuration configuration = null;
        configuration = HBaseConfiguration.create();

        configuration.set("hbase.rootdir", "hdfs://master:8020/HBase_DB");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        */


        /*配置操作HDFS

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://master:8020");
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

            FileSystem fs = FileSystem.get(conf);
            DataInputStream getIt = fs.open(new Path("hdfs://master:8020/pzxFile/file.txt"));
            BufferedReader d = new BufferedReader(new InputStreamReader(getIt));
            String content = d.readLine(); //读取文件一行
            System.out.println(content);
            fs.close();

         */



        //直接向hadoop提交mapreduce
        Configuration configuration = new Configuration();

        //打包成放到hadoop中运行不需要配置

        //远程提交mapreduce作业，缺一不可
        configuration.set("fs.defaultFS", "hdfs://master:8020/");
        configuration.set("mapreduce.job.jar", "C:\\Users\\PZX\\Desktop\\Potree2HBase\\out\\artifacts\\Potree2HBase_jar2\\Potree2HBase.jar");
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("yarn.resourcemanager.hostname", "master");
        configuration.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration.set("mapreduce.app-submission.cross-platform", "true");


        Job job = new Job(configuration,"word count") ;


        job.setJarByClass(WordCount.class); // 主类
        job.setMapperClass(WordMapper.class); // Mapper
        job.setCombinerClass(WordReducer.class); //作业合成类
        job.setReducerClass(WordReducer.class); // reducer
        job.setOutputKeyClass(Text.class); // 设置作业输出数据的关键类
        job.setOutputValueClass(IntWritable.class); // 设置作业输出值类

        FileInputFormat.addInputPath(job,new Path("hdfs://master:8020/pzxFile")); //文件输入
        FileOutputFormat.setOutputPath(job,new Path("hdfs://master:8020/output")); // 文件输出

        System.exit(job.waitForCompletion(true) ? 0 : 1); //等待完成退出
    }



}