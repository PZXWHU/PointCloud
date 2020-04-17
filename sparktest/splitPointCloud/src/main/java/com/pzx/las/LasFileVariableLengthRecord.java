package com.pzx.las;

import com.pzx.utils.LittleEndianUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.nio.MappedByteBuffer;

public class LasFileVariableLengthRecord {

    private Logger logger = Logger.getLogger(LasFilePointData.class);
    private MappedByteBuffer variableLengthRecordBuffer;
    private long numberOfVariableLengthRecords;


    public LasFileVariableLengthRecord(MappedByteBuffer variableLengthRecordBuffer,long numberOfVariableLengthRecords){
        this.variableLengthRecordBuffer = variableLengthRecordBuffer;
        this.numberOfVariableLengthRecords = numberOfVariableLengthRecords;

        for(long i = 0;i<numberOfVariableLengthRecords;i++){
            readBytes(2);//reserved
            try {
                String userID = new String(readBytes(16),"ISO8859-1");
                System.out.println(userID);
            }catch (UnsupportedEncodingException e){
                e.printStackTrace();
            }
            int recordID = LittleEndianUtils.bytesToUnsignedShort(readBytes(2));
            System.out.println(recordID);
            int variableRecordLength = LittleEndianUtils.bytesToUnsignedShort(readBytes(2));
            try {
                String userID = new String(readBytes(32),"ISO8859-1");
                System.out.println(userID);
            }catch (UnsupportedEncodingException e){
                e.printStackTrace();
            }
            readBytes(variableRecordLength);

        }


    }

    private byte[] readBytes(int size){
        byte[] bytes = new byte[size];
        variableLengthRecordBuffer.get(bytes);
        return bytes;
    }

}
