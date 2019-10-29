package HBaseAPI.DML;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class ScanAPI {

    void sacnAPI(){
        try{

            HTable hTable = null;

            //获取扫描结果
            ResultScanner results = hTable.getScanner(new Scan());
            hTable.getScanner("family".getBytes());
            hTable.getScanner("family".getBytes(),"col".getBytes());//隐式创建scan实例，逻辑上调用了第一个方法。

            //构造扫描器
            Scan scan =new  Scan();
            scan = new Scan("startRow".getBytes());
           // scan = new Scan("startRow".getBytes(),filter);
            scan = new Scan("startRow".getBytes(),"endrow".getBytes());

            //添加限制条件
            //scan.addFamily();
            //scan.addColumn();
            scan.setTimeRange(1,2);
            scan.setTimeStamp(1);
            scan.setMaxVersions();
            scan.setMaxVersions(1);//与getAPI一样

            //scan.setStartRow();
            //scan.setStopRow();
            //scan.setFilter();
            //scan.hasFilter();

            scan.setCacheBlocks(true);//设置读缓存，与get相同


        }catch (Exception e){

        }



    }

}
