package com.pzx.las;

public class ByteConverter {

    public static int unsignedByteToInteger(byte b){
        return  b&0xff;
    }

    public static short unsignedByteToShort(byte b){
        return  (short)(b&0xff);
    }


    public static void main(String[] args){
        byte b1 = -128;
        byte b2 = 1;
        short s = 31241;
        //System.out.println(Integer.toBinaryString(b1&s));
        //System.out.println(Integer.toBinaryString((int)b1&(int)s));
        System.out.println(unsignedByteToShort(b1));
    }

}
