package HBaseAPI.DML;

import org.apache.hadoop.hbase.client.Result;

public class ResultAPI {

    void resultAPI(){
        Result result = null;

        result.getValue("family".getBytes(),"col".getBytes());//获取此单元格的最新版本
        result.value();//获取第一个列最新单元格的值
        result.getRow();//返回行键
        result.size();//返回的keyvalue的数目
        result.isEmpty();
        result.rawCells();//返回底层keyvalue数组
        result.listCells();//返回底层keyvalue列表list


        //面向列的函数
        result.getColumnCells("family".getBytes(),"col".getBytes());//返回一个列的所有版本
        result.getColumnLatestCell("family".getBytes(),"col".getBytes());//返回对应列的最新版本，keyvalue
        result.containsColumn("family".getBytes(),"col".getBytes());

    }
}
