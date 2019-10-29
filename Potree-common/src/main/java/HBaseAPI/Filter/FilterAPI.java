package HBaseAPI.Filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.List;

public class FilterAPI {

    void filterAPI(){

        //比较运算符
        //LESS,LESS_OR_EQUAL,EQUAL,NOT_EQUAL,GREATER_OR_EQUAL,GRATER,NO_OP

        //比较器
        //BinaryComparator  使用Bytes.compareTo(),按字节索引顺序比较指定字节数组,
        //BinaryPrefixComparator //与上面相似，但是是从左端开始前缀匹配
        //NullComparator //不做匹配，只判断当前值是不是null
        //BitComparator //通过BitwiseOp类提供的按位and，or，xor操作执行位级比较
        //RegexStringComparator //根据一个正则表达式，在实例化这个比较器时去匹配表中的数据
        //SubstringComparator //把阈值和表中的数据当作string实例，同时通过contains操作匹配字符串
        //后面三种比较器只能与EQUAL,NOT_EQUAL搭配使用
        //通常每个比较器都有一个带比较值参数的构造函数



        //1.比较过滤器

        //行过滤器
        Filter filter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes("row_22")));
        filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator(".*-.5"));

        //列族过滤器
        //FamilyFilter

        //列名过滤器
        //QualifierFilter

        //值过滤器
        //ValueFilter

        //参考列过滤器，允许用户指定一个参考列，参考列过滤器使用参考列的时间戳，并在过滤时包含所有与引用时间戳相同的列
        //DependentColumnFilter
        //参考列过滤器与扫描操作的批量处理功能不兼容




        //2.专用过滤器

        //单列值过滤器
        SingleColumnValueFilter singleColumnValueFilter = null;
        //用一列的值决定是否一行数据被过滤
        singleColumnValueFilter.setFilterIfMissing(true);//设置当参考列不存在时，过滤掉这一行，默认不过滤
        singleColumnValueFilter.setLatestVersionOnly(false);//设置过滤器只检查参考列的最新版本，设置为false后会检查所有版本

        //单列排除过滤器
        //SingleColumnValueExcludeFilter
        //使用方法与上面相同，不同的是结果不包含参考列

        //前缀过滤器
        PrefixFilter prefixFilter = new PrefixFilter("pre".getBytes());
        //所有与前缀匹配的行都会被返回到客户端

        //分页过滤器
        //对结果进行分页，每次查询只返回固定的行数
        //PageFilter


        //行键过滤器
        //KeyOnlyFilter
        //只将结果中的keyvalue实例的键返回，而不需要返回的数据

        //首次行键过滤器
        //FirstKeyOnlyFilter
        //之访问一行中的第一列

        //包含结束的过滤器
        //InclusiveStopFilter
        //终止行被包括在结果内

        //时间戳过滤器
        //TimestampsFilter
        //传入一个装载了时间戳的List实例
        //返回符合list中时间戳的结果

        //列计数过滤器
        //ColumnCountGetFilter
        //用来限制每行最多取回多少列，适合get

        //列分页过滤器
        //ColumnPaginationFilter(int limit,int offset)
        //跳过所有偏移量小于offset的列，并包括之后所有偏移量在limit之前的列

        //列前缀过滤器
        //ColumnPrefixFilter

        //随机行过滤器
        //RandomRowFilter(float chance)
        //随机决定行是否被过滤




        //3.附加过滤器

        //跳转过滤器
        //包装一个用户提供的过滤器，当包装的过滤器遇到一个需要过滤的keyvalue时，那么整行数据都会被过滤掉
        //SkipFilter

        //全匹配过滤器
        //当一条数据被过滤掉时，他就会直接放弃这次扫描操作
        //WhileMatchFilter

        //4.过滤器列表
        FilterList filterList = null;
        //过滤器包含两个参数operator和List《Filter》
        //oprator 取值 MUST_PASS_ALL  必须通过所有过滤器  ；MUST_PASS_ONE，通过一个过滤器就会包含在结果中

        //filterList.addFilter();
        //可以添加一个filter，也可以添加一个filterList，每一个filterList只能有一个操作符





        //自定义过滤器
        //继承Filter接口或者继承FilterBase类，后者为接口中的所有成员提供了默认实现
        Filter filter1 = new FilterBase() {
            // enum ReturnCode{
            //    INCLUDE, 在结果中包含这个keyvalue
            //    SKIP,  跳过这个keyvalue，继续处理接下来的工作
            //    NEXT_COL, 跳过当前列处理下一个列
            //    NEXT_ROW,  跳过当前行处理下一行
             //   SEEK_NEXT_USING_HINT  一些过滤器需要跳过一系列值，此时这个返回值通知使用getNextKetHint来决定跳到什么位置
           // }

            @Override
            public void reset() throws IOException {
                //第五个被执行
                //在迭代扫描中为每个新行重置过滤器。服务器端读取一行数据后，这个方法会被隐式的调用
                //对get无效，因为get只取单读一行
            }

            @Override
            public boolean filterRowKey(byte[] bytes, int i, int i1) throws IOException {
                //第一个被执行
                //检查行键
                return false;
            }

            @Override
            public boolean filterAllRemaining() throws IOException {
                //第六个被执行
                //当这个方法返回true时，用于结束整个扫描
                return false;
            }

            @Override
            public ReturnCode filterKeyValue(Cell cell) throws IOException {
                //第二个被执行
                //检查这一行中的每个keyvalue，同时按照ReturnCode处理当前值
                return null;
            }

            @Override
            public Cell transformCell(Cell cell) throws IOException {
                return null;
            }

            @Override
            public KeyValue transform(KeyValue keyValue) throws IOException {
                return null;
            }

            @Override
            public void filterRowCells(List<Cell> list) throws IOException {
                //第三个执行
                //一旦所有的行和列经过前两个方法检查后，这个方法就会被调用。
                //本方法访问前两个方法筛选出来的keyvalue实例
            }

            @Override
            public boolean hasFilterRow() {
                //一个过滤器要使用filterRow或者filterRow（List），必须重载此函数，返回true
                return false;
            }

            @Override
            public boolean filterRow() throws IOException {
                //第四个执行

                return false;
            }

            @Override
            public KeyValue getNextKeyHint(KeyValue keyValue) throws IOException {
                return null;
            }

            @Override
            public Cell getNextCellHint(Cell cell) throws IOException {
                return null;
            }

            @Override
            public boolean isFamilyEssential(byte[] bytes) throws IOException {
                return false;
            }

            @Override
            public byte[] toByteArray() throws IOException {
                return new byte[0];
            }
        };


    }


}
