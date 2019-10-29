package HBaseAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

public class HBaseUtils {

    public static Configuration configuration = null;
    public static Connection connection = null;
    public static Admin admin = null;
    private static Logger log = Logger.getLogger("hbaseutilslog");

    public static void init(){
        configuration = HBaseConfiguration.create();
        /*虚拟机HBase
        configuration.set("hbase.zookeeper.quorum","master");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.rootdir","hdfs://master:9000/hbase");

        */


        //公司Hbase
        //configuration.set("hbase.rootdir", "hdfs://master:9000/hbase");
        /*
        configuration.set("hbase.rootdir", "hdfs://master:8020/HBase_DB");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        */

        //hlx Hbase
        configuration.set("hbase.rootdir", "hdfs://master:9000/HBase_DB");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2,slave3");



        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
            log.info("连接成功！");
        }catch (Exception e){
            log.info("连接失败！");
            System.out.println("连接失败！");
            e.toString();
        }
    }

    public static void close(){
        try{
            if(admin!=null)
                admin.close();
            if(connection!=null)
                connection.close();
        }catch (Exception e){
            e.printStackTrace();
            log.info("关闭失败");
        }
    }

    public static void createTable(String myTableName,String[] colFamilys){
        //init();
        try{
            TableName tableName = TableName.valueOf(myTableName);
            if(admin.tableExists(tableName)){
                log.info("创建表：表已存在！");
            }else{
                /* 2.0 API
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);//2.x API
                for(String colFamily:colFamilys){
                    tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(colFamily)).build());
                }
                admin.createTable(tableDescriptorBuilder.build());

                 */
                HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(myTableName));
                for (String familyName:colFamilys){
                    hbaseTable.addFamily(new HColumnDescriptor(familyName));
                }
                admin.createTable(hbaseTable);
                log.info("创建表成功");
            }

            /*
            1.x API
            if(!admin.isTableAvailable(TableName.valueOf(tableName))){
                 HTableDescriptor hbaseTable = new HTableDescriptor(TableName.valueOf(tableName));
                for (String familyName:familyNames){
                     hbaseTable.addFamily(new HColumnDescriptor(familyName));
                 }
                admin.createTable(hbaseTable);
             }
             */

        }catch (Exception e){
            log.severe(e.toString());
            log.info("创建表失败");
        }
    }

    public static void deleteTable(String tableName){
        //init();
        TableName tn = TableName.valueOf(tableName);
        try{
            if(admin.tableExists(tn)){
                admin.disableTable(tn);
                admin.deleteTable(tn);
                log.info("删除表成功");
            }else{
                log.info("删除表：表不存在");
            }
        }catch (Exception e){
            log.info("删除表失败");
            log.severe(e.toString());
        }
    }

    public static void listAllTables(){
        //init();
        try{
            /*
            2.x API
            List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
            for(TableDescriptor tableDescriptor:tableDescriptors){
                System.out.println(tableDescriptor.getTableName());
            }

             */

            //1.0 API
            HTableDescriptor[] hTableDescriptors = admin.listTables();
            for(HTableDescriptor hTableDescriptor:hTableDescriptors){
                System.out.println(hTableDescriptor.getTableName());
            }


        }catch (Exception e){
            log.severe(e.toString());
            log.severe("列出全部表失败");
        }
    }

    public static void insertRow(String tableName,String rowKey,String colFamily,String col,String val){
        //init();
        try{
            Table table = connection.getTable(TableName.valueOf(tableName));

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col),Bytes.toBytes(val));

            table.put(put);
            table.close();
            log.info("插入成功");
        }catch (Exception e){
            log.severe(e.toString());
            log.info("插入失败");
        }
    }

    public static  void deleteRow(String tableName,String rowKey,String colFamily,String col){
        //init();
        try{
            Table table = connection.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
            table.close();
            log.info("删除表成功");
        }catch (Exception e){
            log.severe(e.toString());
            log.info("删除失败");
        }
    }

    public static byte[] getData(String tableName,String rowKey,String colFamily,String col){
        //init();
        try{

            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
            Result result = table.get(get);
            byte[] bytes = result.getValue(colFamily.getBytes(),col.getBytes());
            //showCell(result);
            table.close();
            //log.info("获取数据成功");
            return bytes;
        }catch (Exception e){
            log.severe(e.toString());
            log.info("获取数据失败");
            return null;
        }
    }

    public static void showCell(Result result){
        Cell[] cells = result.rawCells();
        for(int i=0;i<cells.length;i++){
            Cell cell = cells[i];
            System.out.println("RowName:"+new String(CellUtil.cloneRow(cell)));
            System.out.println("Timetamp:"+new String(cell.getTimestamp()+""));
            System.out.println("colFamily:"+new String(CellUtil.cloneFamily(cell)));
            System.out.println("col Name:"+new String(CellUtil.cloneQualifier(cell)));
            System.out.println("value:"+new String(CellUtil.cloneValue(cell)));
        }
    }

    public static void batch(String myTableName,List<Row> actions){
        Object[] results = new Object[actions.size()];
        try{
            TableName tableName = TableName.valueOf(myTableName);
            Table table = connection.getTable(tableName);


            table.batch(actions,results);

        }catch (Exception e ){
            log.severe(e.toString());
            log.info("批量操作失败");
        }

        for(Object result : results){
            System.out.println(result.toString());
        }


    }

    public static void scan(String tableName){
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            for(Result result:resultScanner){
                System.out.println(new String(result.getRow()));

            }

        }catch (IOException e){
            log.severe(e.toString());
            log.info("扫描错误");
        }

    }



    public static void loadCoprocessor(String tableName,Class coprocessorClass,String coprocessorPath){
        try {
            Table table = HBaseUtils.connection.getTable(TableName.valueOf(tableName));
            HBaseUtils.admin.disableTable(TableName.valueOf(tableName));
            HTableDescriptor hTableDescriptor = table.getTableDescriptor();
            Path path = new Path(coprocessorPath);

            hTableDescriptor.addCoprocessor(coprocessorClass.getCanonicalName(),path, Coprocessor.PRIORITY_USER,null);
            HBaseUtils.admin.modifyTable(TableName.valueOf(tableName),hTableDescriptor);
            HBaseUtils.admin.enableTable(TableName.valueOf(tableName));

            log.info("加载协处理器成功!");
        }catch (Exception e){
            log.severe("加载协处理器失败！");
        }

    }

    public static void unloadCoprocessor(String tableName,Class coprocessorClass){
        try {
            Table table = HBaseUtils.connection.getTable(TableName.valueOf(tableName));
            HBaseUtils.admin.disableTable(TableName.valueOf(tableName));
            HTableDescriptor hTableDescriptor = table.getTableDescriptor();

            hTableDescriptor.removeCoprocessor(coprocessorClass.getCanonicalName());

            HBaseUtils.admin.modifyTable(TableName.valueOf(tableName),hTableDescriptor);
            HBaseUtils.admin.enableTable(TableName.valueOf(tableName));

            log.info("卸载协处理器成功");
        }catch (Exception e){
            log.severe("卸载协处理器失败！");
        }

    }


    public static void deleteColumnData(Table table,String colFamily,String colName){
        try {
            Scan scan = new Scan();
            scan.addColumn(colFamily.getBytes(),colName.getBytes());
            ResultScanner rs = table.getScanner(scan);
            Iterator<Result> iterator = rs.iterator();
            ArrayList<Delete> deletes = new ArrayList<>();
            while (iterator.hasNext()){
                Delete delete = new Delete(iterator.next().getRow());
                delete.addColumn(colFamily.getBytes(),colName.getBytes());
                deletes.add(delete);
            }
            table.delete(deletes);


        }catch (Exception e){
            log.severe(e.toString());
        }


    }



}
