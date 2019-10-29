package HBaseAPI.DDL;

import org.apache.hadoop.hbase.HColumnDescriptor;

public class ColumnFamilyDescriptorAPI {

    void columnFamilyDescriptorAPI(){
        try {

            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("colFamily");
            //构造函数还可包括参数
            //maxversion 最大版本数
            //compression 压缩方法
            //ismemory
            //blockcacheenabled 是否开启读缓存
            //blocksize 块大小
            //timetolive 存活时间


            //setter和getter函数
            hColumnDescriptor.getName();
            //列族不能重新命名，通常做法是新建一个列族，然后使用api从旧列族复制数据到新列祖
            //列族名必须是可见字符

            hColumnDescriptor.setMaxVersions(1);//默认为3

            //hColumnDescriptor.setCompressionType();//设置压缩类型，默认为NONE

            hColumnDescriptor.setBlocksize(1);//hbase储存文件被划分为块，默认64kb
            //这个参数指定了hbase一次读取过程中顺序读取多少数据到内存缓冲区

            hColumnDescriptor.setBlockCacheEnabled(true);//默认为true

            hColumnDescriptor.setTimeToLive(1);//版本数据保存时间，默认永久保存

            hColumnDescriptor.setInMemory(false);///默认为false
            //设置这个参数为true并不意味着将整个列族的所有储存快加载到内存中
            //而是在正常读取过程中，数据块被加载到缓存中并长期驻留在内存中，除非堆压力过大，才会强制卸载这部分数据

            //布隆过滤器
            //布隆过滤器能减少特定访问模式下的查询时间，但是会增加内存和储存的负担
            //默认为关闭状态
            //有三种类型，NONE,ROW,ROWCOL
            //hColumnDescriptor.setBloomFilterType();

            //复制范围
            //跨集群同步，本地集群的数据更新可以及时同步到其他集群
            //取值0，1
            hColumnDescriptor.setScope(1);
            //0表示本地范围，即关闭实时同步
            //1表示全局范围，开启实时同步
            //默认为0




        }catch (Exception e){

        }
    }

}
