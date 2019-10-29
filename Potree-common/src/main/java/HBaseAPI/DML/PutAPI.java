package HBaseAPI.DML;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PutAPI {

    public static void putAPI(){
        try{
            //创建put实例 有4种
            Put put  = new Put(Bytes.toBytes("rowKey"));

            //向put添加数据
            put.addColumn(Bytes.toBytes("family"),Bytes.toBytes("col"),Bytes.toBytes("values"));
            put.add(new KeyValue());

            //向put获取数据
            List<Cell> cells = put.get(Bytes.toBytes("family"),Bytes.toBytes("col"));
            put.getFamilyCellMap();

            //put是否存在特定单元格 4种
            boolean is = put.has(Bytes.toBytes("family"),Bytes.toBytes("col"));
            is = put.has(Bytes.toBytes("family"),Bytes.toBytes("col"),Bytes.toBytes("values"));

            //其他方法
            put.getRow();
            put.setWriteToWAL(true);
            put.getWriteToWAL();
            put.getTimeStamp();
            put.isEmpty();
            put.size();

            //写缓冲区
            HTable hTable =null;
            hTable.setAutoFlush(false);//缓冲区默认禁用,false设置不进行自动刷写，激活缓冲区
            hTable.isAutoFlush();
            hTable.flushCommits();//强制刷写
            hTable.setWriteBufferSize(21221);//设置缓冲区大小，超过缓冲区大小就会自动刷写
            hTable.getWriteBufferSize();

            //put列表
            List<Put> putList = new ArrayList<>();
            Put put1 = new Put("da".getBytes());
            putList.add(put);putList.add(put1);
            hTable.put(putList);//批量操作
            //当列表里的put在服务端执行时，没有异常的put正常执行，出现异常的put会被保存在本地写缓冲区里，
            //下一次缓冲区刷写时重试。
            //而有一些检查是在客户端完成的，比如put实例的内容是否为空或者是否指定了列，在这种情况下，
            //客户端爆出异常，之后的put都不会被插入写缓冲区，但是之前的put已经插入到写缓冲区种
            //服务器调用的顺序不受用户控制！！！1

            //原子操作
            hTable.checkAndPut("row".getBytes(),"family".getBytes(),"col".getBytes(),"vaule".getBytes(),put);


        }catch (Exception e){

        }



    }

}
