package HBaseAPI.DML;

import org.apache.hadoop.hbase.client.Delete;

public class DeleteAPI {
    void deleteAPI(){
        Delete delete = new Delete("row".getBytes());

        //限制条件
        //delete.addColumns();//删除列祖下面所有列
        //delete.addFamily();//删除具体列
        //若指定了时间戳，则会删除比时间戳旧或者相等的版本
        //若不指定时间戳，则会删除所有版本

        //delete.addColumn()
        //删除具体列，若指定时间戳，则删除指定时间戳版本，若未指定时间戳，则删除最新版本

        delete.setTimestamp(321);//删除匹配或者比时间戳旧的所有列族种的所有列

        //delete列表
        //相似
        //如果列表里一个delete出错，其他正确的会执行，出错的delete或留在delete列表里

        //原子操作

        //checkanddelete()
    }

}
