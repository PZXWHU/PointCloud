package HBaseAPI.DML;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class ResultScannerAPI {

    void resultScannerAPI(){

        try{

            ResultScanner results = null;

            //获取result
            Result result = results.next();//获取一个result
            Result[] results1 = results.next(10);//获取10个result
            results.close();//扫描器长时间不关闭会超时，超时之后再取数据会异常

            //缓存与批量处理
            Scan scan = new Scan();
            scan.setCaching(10);//每次next（）会生成一次RPC调用，一次RPC调用返回10个result放到客户端缓存
            scan.getCaching();

            scan.setBatch(5);//避免一行有太多列，一个result返回5列，一行所有列不能被5整除，则最后一个result返回较少的列





        }catch (Exception e){

        }




    }

}
