package HBaseAPI.DDL;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;

public class TableDecriptorAPI {

    void tableDescriptorAPI(){

        try {
            HTable hTable = null;
            HTableDescriptor hTableDescriptor = hTable.getTableDescriptor();

            hTableDescriptor = new HTableDescriptor("tableName");

            //setter和getter方法
            hTableDescriptor.setName("tablename".getBytes());
            hTableDescriptor.getNameAsString();

            //关于列族
            hTableDescriptor.hasFamily("family".getBytes());
            hTableDescriptor.addFamily(new HColumnDescriptor());
            hTableDescriptor.getColumnFamilies();//获得列族的列表
            hTableDescriptor.getFamily("family".getBytes());//获得某个列族
            hTableDescriptor.removeFamily("family".getBytes());//删除某个列族

            //文件大小限制，限制了表中region的大小
            //如果一个列族的储存单元已使用的储存空间超过了大小限制，region会发生拆分操作
            hTableDescriptor.setMaxFileSize(1);
            hTableDescriptor.getMaxFileSize();

            //只读
            hTableDescriptor.isReadOnly();
            hTableDescriptor.setReadOnly(true);

            //memstore刷写大小
            hTableDescriptor.setMemStoreFlushSize(1);
            hTableDescriptor.getMemStoreFlushSize();


        }catch (Exception e){

        }

    }

}
