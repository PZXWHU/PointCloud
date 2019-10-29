package com.pzx;

import org.apache.log4j.Logger;

import java.io.*;

public class Test {

    public static Logger logger = Logger.getLogger(Test.class);

    public static void main(String[] args) throws IOException {


        logger.debug("ddddddddddddas");
/*
        File file = new File(Thread.currentThread().getContextClassLoader().getResource("r.bin").getPath());

        FileInputStream fileInputStream = new FileInputStream(file);
        DataInputStream dataInputStream  =  new DataInputStream(fileInputStream);
        byte[] unit8 = new byte[1];
        byte[] unit32 = new byte[4];
        int i=0;
        while (i<3){
            dataInputStream.read(unit32);

            int ch1 = unit32[0]&0x000000FF;
            int ch2 = unit32[1]&0x000000FF;
            int ch3 = unit32[2]&0x000000FF;
            int ch4 = unit32[3]&0x000000FF;

            System.out.println(ch1);
            System.out.println(ch2);
            System.out.println(ch3);
            System.out.println(ch4);

            i++;

            System.out.println((ch1 << 24) | (ch2 << 16) | (ch3 << 8) | (ch4 << 0));
            System.out.println("----------------------");
        }


 */
/*
        double diagonal = Math.pow(Math.pow(562503.37-553512.22,2)+Math.pow(5138032.36-5129041.21,2)+Math.pow(9750.840000000024-759.69,2),1/3);
        System.out.println(diagonal/250);
        System.out.println(Math.pow(Math.pow(562503.37-553512.22,2)+Math.pow(5138032.36-5129041.21,2)+Math.pow(9750.840000000024-759.69,2),1.0/3)/250);

 */
        //System.out.println(562503.37-553512.22);












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
