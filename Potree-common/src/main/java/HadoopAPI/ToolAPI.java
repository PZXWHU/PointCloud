package HadoopAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

public class ToolAPI extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        for(Map.Entry<String,String> entry:conf){
            System.out.printf("%s=%s\n",entry.getKey(),entry.getValue());

        }
        return 0;
    }

    public static void main(String[] args){


        try {
            int exitCode = ToolRunner.run(new ToolAPI(),args);
            System.exit(exitCode);
        }catch (Exception e){
            System.out.println("运行失败");
        }

    }


}
