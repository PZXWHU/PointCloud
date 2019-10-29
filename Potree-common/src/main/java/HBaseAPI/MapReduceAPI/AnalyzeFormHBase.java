package HBaseAPI.MapReduceAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class AnalyzeFormHBase {

    public enum  Counters{ROWS,CLOS};

    static class AnalyzeMapper extends TableMapper<Text, IntWritable>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            context.getCounter(Counters.ROWS).increment(1);

            String v = null;
            try {
                for(Cell cell :value.listCells()){
                    context.getCounter(Counters.CLOS).increment(1);
                    v = Bytes.toStringBinary(cell.getValueArray());

                    context.write(new Text("das"),new IntWritable(1));

                }

            }catch (Exception e){

            }


        }
    }


    static class AnalyzeReducer extends Reducer<Text, IntWritable,Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;
            for(IntWritable one: values)
                count++;


            context.write(key,new IntWritable(count));
        }
    }

    public static void main(){



        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","master");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.rootdir","hdfs://master:9000/hbase");

        Scan scan = new Scan();
        String table = null;
        String output = "";

        try {
            Job job = Job.getInstance(configuration,"Analyze Job");
            job.setJarByClass(AnalyzeFormHBase.class);

            //设置map和reduce类
            job.setMapperClass(AnalyzeMapper.class);
            job.setReducerClass(AnalyzeReducer.class);

            //设置输出输出格式
            job.setInputFormatClass(TableInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            //设置MapReduce输入输出具体路径
            TableMapReduceUtil.initTableMapperJob(table,scan,AnalyzeMapper.class,Text.class,IntWritable.class,job);
            FileOutputFormat.setOutputPath(job,new Path(output));

            boolean b = job.waitForCompletion(true);

            if(!b)
                System.out.println("job failed");

        }catch (Exception e){

        }




    }



}
