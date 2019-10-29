package HBaseAPI.DML;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;

public class IncrementAPI {

    void incrementAPI(){
        try {

            //用户不用初始化计数器，当用户第一次使用计数器时，计数器自动被设为0
            //用户也可以直接读写一个计数器，不过必须保证计数器时long型
            //计数器就是和其他列类似的简单列

            //API由HTable提供
            HTable hTable = null;

            //单计数器
            hTable.incrementColumnValue("rowkey".getBytes(),"family".getBytes(),"col".getBytes(),1L);


            //多计数器，工作模式与CRUD操作类似
            Increment increment = new Increment("rowkey".getBytes());
            hTable.increment(increment);
            //读操作不需要获取锁，所以它可能读到一行中被修改到一半的数据！
            //scan和get操作同样会出现这种情况。

            increment.addColumn("family".getBytes(),"col".getBytes(),1);
            //计数器版本都被隐式处理

            increment.setTimeRange(1,2);


        }catch (Exception e){

        }




    }

}
