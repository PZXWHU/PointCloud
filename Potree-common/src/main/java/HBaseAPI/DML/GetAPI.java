package HBaseAPI.DML;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;

import java.util.ArrayList;
import java.util.List;

public class GetAPI {

    public static void getAPI(){

        try {

            //2种
            Get get = new Get("row".getBytes());

            //限制条件
            get.addFamily("family".getBytes());
            get.addColumn("family".getBytes(),"col".getBytes());
            get.setTimeRange(1,2);
            get.setTimeStamp(1);
            get.setMaxVersions();//获取所有版本的数据
            get.setMaxVersions(3);//获取3个版本
            //不设置版本数就是获取最新版本

            //其他方法
            get.setFilter(null);
            get.setCacheBlocks(true);//设置region是否缓存读取的数据

            //get列表
            HTable hTable = null;
            Get get1 = null;
            List<Get> getList = new ArrayList<>();
            getList.add(get);getList.add(get1);
            hTable.get(getList);
            //一个get操作出现异常，不会有数据返回

            //获取数据的其他方法
            hTable.exists(get);//只检测是否存在数据，不返回

            //特殊检索方法
            hTable.getRowOrBefore("family".getBytes(),"col".getBytes());//获取一个特定的行或者请求行之前的一行


        }catch (Exception e){

        }


    }
}
