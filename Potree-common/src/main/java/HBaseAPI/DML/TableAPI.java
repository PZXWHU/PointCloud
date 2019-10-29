package HBaseAPI.DML;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

public class TableAPI {
    void tableAPI(){
        try {

            HTable table = null;

            table.close(); //此操作会刷写所有客户端缓冲的写操作，隐式调用flushCache（）
            table.getName();
            table.getConfiguration();
            table.getTableDescriptor();

            table.getStartKeys();
            table.getEndKeys();
            table.getStartEndKeys();

            table.clearRegionCache();//清理缓存中region的位置
            table.getRegionLocator();//获取region的位置信息



        }catch (Exception e){

        }


    }
}
