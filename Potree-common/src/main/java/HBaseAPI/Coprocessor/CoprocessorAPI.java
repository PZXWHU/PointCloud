package HBaseAPI.Coprocessor;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;


import java.io.IOException;

public class CoprocessorAPI {

    void coprocessAPI(){
        try {


            Coprocessor coprocessor = new Coprocessor() {

                //协处理器优先级
                //Coprocessor.PRIORITY_HIGHEST;等等


                @Override
                public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
                    //协处理器开始时调用
                }

                @Override
                public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
                    //协处理器结束时调用
                }
            };

            //协处理器的环境
            CoprocessorEnvironment coprocessorEnvironment = null;

            coprocessorEnvironment.getHBaseVersion();//获得Hbase版本
            coprocessorEnvironment.getVersion();//获得coprecessor接口的版本
            coprocessorEnvironment.getInstance();//获得加载的协处理器实例
            coprocessorEnvironment.getPriority();//返回协处理器的优先级
            coprocessorEnvironment.getLoadSequence();//返回协处理器的序号
            coprocessorEnvironment.getTable(TableName.valueOf("tablename"));
            //获取表的实例，允许协处理器访问实际的表数据


            //协处理器实例生命周期
            //Coprocessor.State.UNINSTALLED;//协处理器最初的状态，没有环境，也没有被初始化
            //Coprocessor.State.INSTALLED;//协处理器实例装载了它的环境参数
            //Coprocessor.State.STARTING;//协处理器将要开始工作，也就是说start（）方法将被调用
            //Coprocessor.State.ACTIVE;//一旦start（）方法被调用，当前状态被设置为active
            //Coprocessor.State.STOPPING;//stop（）方法被调用之前的状态
            //Coprocessor.State.STOPPED;//一旦stop（）方法将控制权交给框架，协处理器会被设置为状态stopped


            //协处理器加载
            //1.从配置文件中加载
            //用户通过在hbase-site.xml文件中注册自己的协处理器类
            //配置文件中的顺序非常重要，决定了执行顺序
            //所有的协处理器都是以系统级优先级进行加载的
            //默认的协处理器会被每张表和每个region加载

            //2.从表描述符中加载
            //因为是针对特定表的，所以加载协处理器只针对这个表的region
            HTableDescriptor hTableDescriptor = null;
            //hTableDescriptor.setValue();必须使用此种方法进行定义
            //键必须以COPROCESSOR开头，值必须符合path-to-jar|classname|priority
            //使用后缀$<number>后缀可以改变定义中的顺序，即协处理器加载的顺序



            //RegionObserver类
            //region级别，当一个特定region级别的操作发生时，他们的钩子函数就会被触发
            //这些操作可以分成两类：region生命周期变化和客户端api调用

            //1.处理region生命周期事件
            //状态：pending open
            //preOpen，postOpen，这些方法会在region被打开前或者刚刚打开后被调用

            //状态：open
            //preFlush。。。

            //状态：pending close
            //preClose

            //2.处理客户端api事件
            //preGet，postGet....

            //RegionCoprocessorEnvironment类


            //ObserverContext类
            //RegionObserver类提供的所有回调函数都需要一个特殊的上下文作为共同的参数
            //不仅提供当前系统环境的入口，同时添加了一些关键功能通知回调函数完成时需要做什么

            //BaseRegionObserver类
            //实现监听类型协处理器的基类，实现了RegionObserver接口的空方法




            //MasterObserver类
            //用于处理服务器的所有回调函数




            //endpoint
            //regioncoprocessor：计算任务之发送到相关region的服务器上
            //自定义RPC协议，系统提供了一个CoprocessorProtocol接口(Service)
            //通过这个接口可以定义协处理器希望暴露给用户的任意方法
            //通过以下HTable提供的调用方法，使用这个这个协议可以和协处理器实例之间通信



            HTable hTable = null;
            //hTable.coprocessorService();此方法使用单个行键，返回一个动态代理，它使用包含给定行键的region作为RPC endpoint
            //也可以使用起始行键和终止行键。表中包含在起始行键到终止行键（不包含终止行键）范围内的所有region都将作为RPC endpoint
            //作为参数被传入HTable的方法中的行键不会传入CoprocessorProtocal的实现中，而是仅仅用于确定远端调用endpoint的region

            //Batch类为CoprocessorProtocol中涉及多个region方法的调用定义了两个接口
            //客户端实现了Batch.call方法，每个选中的region将会调用一次这个call方法，并将coprocessorprotocol实例作为region的参数
            //在调用完成时，客户端可以选择实现Batch.Callback来传回每次region调用的结果
            //void update(byte[] region,byte[] row,R result)
            //以上方法被调用时将使用以下函数R call（T instance）返回的值作为参数，并且每个region都会调用并返回



            //协处理器的部署
            //1.静态部署
            //通过修改hbase配置文件，并且将打包好的协处理器的jar包放到habse的classpath路径中，重启hbase即可
            //这种方式加载的协处理器是全局的，所有表所有region都有，并且时系统级别的优先级

            //2.动态部署
            //通过表描述符HTableDescripot.setValue()进行设置，需要将jar包放到hdfs中，设置时需要disable表，设置完成enbale表，即可
            //此种加载是针对单个表的，只有此表的region会拥有这种协处理器







        }catch (Exception e){

        }




    }

}
