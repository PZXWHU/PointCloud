package com;

import org.apache.log4j.Logger;

import java.io.*;

public class Test {

    public static Logger logger = Logger.getLogger(Test.class);

    public static void main(String[] args) throws IOException {


        FileInputStream inputStream  = new FileInputStream("D:\\wokspace\\Potree\\readLAS\\src\\test.bin");
        System.out.println(inputStream.available());
        byte[] bytes = new byte[10];
        inputStream.read(bytes);
        for(byte b :bytes){
            System.out.println(b);
        }


    }

    public static  void bytesReverse(byte[] bytes){
        int length = bytes.length;
        for(int i=0;i<length/2;i++){
            byte b = bytes[i];
            bytes[i] = bytes[length-i-1];
            bytes[length-i-1] = b;
        }
    }

}
