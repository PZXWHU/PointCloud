import com.pzx.LasSplit;
import com.pzx.distributedLock.DistributedRedisLock;
import org.apache.log4j.Logger;
import org.apache.log4j.net.SocketServer;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Test {

    public static Logger logger = Logger.getLogger(Test.class);

    public static void main(String[] args) throws Exception {


        ServerSocket server = new ServerSocket(8888);
        Socket socket = server.accept();



        // 建立好连接后，从socket中获取输入流，并建立缓冲区进行读取
        OutputStream outputStream = socket.getOutputStream();
        String message="你好  yiwangzhibujian";
        socket.getOutputStream().write(message.getBytes("UTF-8"));
        outputStream.close();
        Thread.sleep(5000);
        InputStream inputStream = socket.getInputStream();
        byte[] bytes = new byte[1024];
        int len;
        StringBuilder sb = new StringBuilder();

        while ((len = inputStream.read(bytes)) != -1) {
            //注意指定编码格式，发送方和接收方一定要统一，建议使用UTF-8
            sb.append(new String(bytes, 0, len,"UTF-8"));
        }
        System.out.println("get message from client: " + sb);
        inputStream.close();
        socket.close();
        server.close();




    }

    public static  void bytesReverse(byte[] bytes){

    }

}
