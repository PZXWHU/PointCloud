package HBaseAPI.DDL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class HBaseAdminAPI {

    void hbaseAdminAPI(){

        try {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(new Configuration());

            hBaseAdmin.isMasterRunning();

            hBaseAdmin.getConnection();
            hBaseAdmin.getConfiguration();
            hBaseAdmin.close();



            //表操作
            //创建表
            HTableDescriptor hTableDescriptor = null;
            hBaseAdmin.createTable(hTableDescriptor);
            //预分区
            hBaseAdmin.createTable(hTableDescriptor, Bytes.toBytes(1L),Bytes.toBytes(100L),10);
            //预分区10个region，第一个region的endkey为1，第10个region的startkey为100
            hBaseAdmin.createTable(hTableDescriptor,new byte[10][]);
            //Hbase第一个region的起始键和最后一个region的终止行键都为空字节
            //前一个region的终止行键和后一个region的起始行键是串联起来的
            //即终止行键不包含在前一个region中，而是作为起始行键包含在后一个region中
            hBaseAdmin.createTableAsync(hTableDescriptor,new byte[10][]);//异步建表

            hBaseAdmin.tableExists("tablename");//检查表是否存在
            hBaseAdmin.listTables();//获取所有的表描述对象
            hBaseAdmin.getTableDescriptor("tablename".getBytes());

            hBaseAdmin.deleteTable("tablename");
            //删除表之前。用户需要禁用表
            hBaseAdmin.disableTable("tablename");
            hBaseAdmin.disableTableAsync("tablename");

            hBaseAdmin.enableTable("tablename");
            hBaseAdmin.enableTableAsync("tablename");

            //检查表的状态
            hBaseAdmin.isTableEnabled("tablename");
            hBaseAdmin.isTableDisabled("tablename");
            hBaseAdmin.isTableAvailable("tablename");





            //创建表之后需要修改表结构
            hBaseAdmin.modifyTable("tablenanme".getBytes(),hTableDescriptor);
            //首先要禁用表，且只提供了异步方式，客户端需循环调用gettabledescriptor检查

            HColumnDescriptor hColumnDescriptor = null;
            hBaseAdmin.addColumn("tablename",hColumnDescriptor);
            hBaseAdmin.deleteColumn("tablename","colfamilyName");
            hBaseAdmin.modifyColumn("tablename",hColumnDescriptor);

            //增加和修改都需要一个HColumnDescriptor实例
            hBaseAdmin.getTableDescriptor("tablename".getBytes()).getColumnFamilies();
            hBaseAdmin.getTableDescriptor("tablename".getBytes()).getFamily("col".getBytes());

            //表的创建、修改都可以基于一个文件
            //Hush模式





            //集群管理
            hBaseAdmin.getClusterStatus();//获取集群信息

            hBaseAdmin.closeRegion("regionname","hostandport");
            //让region服务器中上线的特定region下线

            hBaseAdmin.flush("");
            //将表或者region中的memstore刷写到磁盘上

            hBaseAdmin.compact("");
            hBaseAdmin.majorCompact("");
            //表里的所有region或者特定region执行合并操作

            hBaseAdmin.split("");
            hBaseAdmin.split("","");
            //表里的所有region或者特定region执行拆分操作



            //集群状态信息

            ClusterStatus clusterStatus = hBaseAdmin.getClusterStatus();//集群信息
            //通过ClusterStatus查看集群信息

            ArrayList<ServerName> serverNames = (ArrayList)clusterStatus.getServers();//获取当时存活的region服务器列表，包括region服务器的服务，ip，rpc端口，启动时间戳
            //通过ServerName查看region服务器信息

            ServerLoad serverLoad = clusterStatus.getLoad(serverNames.get(0));//获得region服务器当前的负载情况
            //通过ServerLoad可以查看region服务器的负载情况

            Map<byte[],RegionLoad> regionLoadMap = serverLoad.getRegionsLoad();//获得region服务器中每个region的负载情况
            //通过RegionLoad可以查看region的负载情况







        }catch (Exception e){

        }


    }

}
