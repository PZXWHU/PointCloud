package HadoopAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MapReduceAPI {

    public static void main(String[] args){
        //本地运行模式!!!
        //本地运行模式!!!
        //本地运行模式!!!
        //本地运行模式!!!
        //本地运行模式!!!
        //本地运行模式!!!
        try {
            BasicConfigurator.configure(); //自动快速地使用缺省Log4j环境。加入此代码可打印出hadoop日志

            Configuration conf = new Configuration(true);
            //conf.addResource("hdfs-site.xml");
            //conf.addResource("mapred-site.xml");
            //System.out.println(conf.get("mapreduce.framework.name"));
            //System.out.println(conf.get("hadoop.common.configuration.version"));
            System.out.println(conf.get("dfs.replication"));
            System.out.println(conf.get("dfs.namenode.name.dir"));
            System.out.println(conf.get("mapreduce.framework.name"));

            //mapreduce本地执行
            //默认设置
            //conf.set("mapreduce.framework.name","local");
            //conf.set("fs.defaultFS","file:///");


            Job job = Job.getInstance(conf);

            //job.setJar("/home/hadoop/wc.jar");
            job.setJarByClass(MapReduceAPI.class);

            job.setMapperClass(WordcountMapper.class);
            job.setReducerClass(WordcountReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //指定最终输出的数据类型
            //job.setOutputKeyClass(Text.class);
            //job.setOutputValueClass(IntWritable.class);


            //指定job的输入原始文件目录
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));


            //将job配置的参数，以及job所用的java类所在的jar包提交给yarn去运行
            //job.submit();
            boolean res = job.waitForCompletion(true);
            System.exit(res ? 0 : 1);

        }catch (Exception e){
            System.out.println("出现意外！");
            e.printStackTrace();
        }

        //SequenceFileInputFormat


    }

    private static class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        /*
         * map阶段的业务逻辑就写在自定义的map()
         * maptask会对每一行的输入数据调用自定义的map()方法
         * */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] words = line.split(" ");

            //将单词输出为<单词，1>
            for(String word:words){
                //相同的单词分发给相同的reduce
                context.write(new Text(word),new IntWritable(1));
            }

        }
    }

    private static class WordcountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            /*
             * key--一组相同单词kv对的key
             *
             * */
            int count =0;

            /*
             * Iterator<IntWritable> iterator = values.iterator();
             * while(iterator.hasNext()){
             *   count += iterator.next().get();
             * }
             * */

            for(IntWritable value:values){
                count += value.get();
            }

            context.write(key,new IntWritable(count));

        }
    }


}
