package HBaseAPI.DML;

import org.apache.hadoop.hbase.util.Bytes;

public class BytesAPI {

    void bytesAPI(){

        //Bytes.putLong(bytes,offser,val);//将一个long值写入字节数组的特定位置
        //Bytes.toLong(bytes,offser,length);//将字节数组特定位置，特定长度的字节转换为long值

        Bytes.toStringBinary(new byte[2014]);//与toString很像，但是这种可以安全的将不能打印的信息转换为人工可读的十六进制数
        //Bytes.compareTo();Bytes.equals()  比较
        Bytes.add(new byte[2014],new byte[2014]);//将两个字节数组连接成一个新数组
        Bytes.head(new byte[1025],22);//取字节数组的前22位
        Bytes.tail(new byte[1025],22);//取字节数组的后22位
        //Bytes.binarySearch();二分法查找
        Bytes.incrementBytes(new byte[1024],2);//将一个long数组转化的字节数组与long数据相加，并返回字节数组


    }

}
