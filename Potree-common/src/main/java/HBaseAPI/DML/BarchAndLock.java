package HBaseAPI.DML;

import org.apache.hadoop.hbase.client.*;

import java.util.ArrayList;
import java.util.List;

public class BarchAndLock {

    void f(){

        try {
            //批量操作
            List<Row> batch = new ArrayList<>();
            Put put = null;
            Get get = null;
            Delete delete = null;
            batch.add(put);batch.add(get);batch.add(delete);
            HTable hTable = null;
            Object[] results = new Object[batch.size()];
            hTable.batch(batch,results);
            //batch返回的结果可能是4种  null：通讯失败 emptyresult：put或者delete成功
            //result： get返回   throwbale：出现异常

            hTable.batch(batch);//不需要自己传入数组，但是若是出现异常，一个结果都获取不到

            //批量操作一个操作的失败不会影响其他操作!!!!，且操作的顺序可能会不同！！！
            //delete列表失败的操作会留在delete列表里，put列表失败的操作会在写缓冲区里




        }catch (Exception e){

        }


    }

}
