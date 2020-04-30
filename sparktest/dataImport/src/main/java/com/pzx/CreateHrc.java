package com.pzx;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CreateHrc {

    private static Logger logger = Logger.getLogger(CreateHrc.class);

    public static void main(String[] args) {
        /*
        if ((args.length!=1)){
            System.out.println("请输入表名");
        }
        String tableName = args[0];
        createHrcRow(tableName);

         */

        if ((args.length!=1)){
            System.out.println("请输入文件路径");
            return;
        }
        String outputDirPath = args[0];
        createHrcFile(outputDirPath);

    }


    /**
     * 利用输出目录中所有的bin文件的文件名（即nodeKey），生成索引文件
     * @param outputDirPath 输出目录
     */
    public static void createHrcFile(String outputDirPath){

        List<String> binFilePathList = IOUtils.listAllFiles(outputDirPath).stream().filter((x)->{return x.endsWith(".bin");}).collect(Collectors.toList());
        List<String> nodeKeyList = binFilePathList.stream()
                .map((binFilePath)->binFilePath.substring(binFilePath.lastIndexOf(File.separator)+1,binFilePath.lastIndexOf("."))) //将路径以及文件后缀名截除
                .map(binFile->binFile.substring(binFile.lastIndexOf("r"))) //将层级编码去除
                .collect(Collectors.toList());

        byte[] hrcBytes = createHrcBytes(nodeKeyList);

        try {
            IOUtils.writerDataToFile(outputDirPath+File.separator+"r.hrc",hrcBytes,false);
        }catch (Exception e){
            e.printStackTrace();
        }

    }


    public static byte[] createHrcBytes(List<String> nodeKeyList){
        HashSet<String> nodeKeySet = (HashSet<String>) nodeKeyList.stream().collect(Collectors.toSet());

        nodeKeyList.sort(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                if(o1.length()>o2.length())
                    return 1;
                else if (o1.length()<o2.length())
                    return -1;
                else {
                    return o1.compareTo(o2);
                }
            }
        });
        byte[] hrcBytes = new byte[nodeKeyList.size()];

        for(int i=0;i<nodeKeyList.size();i++){
            String nodeKey = nodeKeyList.get(i);
            byte mask = 0;

            for(int j=0;j<8;j++){
                if(nodeKeySet.contains(nodeKey+j))
                    mask = (byte) (mask|1<<j);
            }
            hrcBytes[i] = mask;
        }

        return hrcBytes;
    }

    public static void createHrcRow(String tableName){
        Filter keyOnlyFilter = new KeyOnlyFilter();
        Map<String,byte[]> resultMap = HBaseUtils.scan(tableName,null,null,"data","bin",keyOnlyFilter);
        //获得的keyString：r3422467-data-bin
        List<String> nodeKeyList = resultMap.keySet().stream()
                .filter(nodeKey -> nodeKey.contains("r"))
                .map(nodeKey -> nodeKey.split("-")[0].substring(1))
                .collect(Collectors.toList());

        byte[] hrcBytes = createHrcBytes(nodeKeyList);
        HBaseUtils.put(tableName,"hrc","data","hrc",hrcBytes);

    }

}
