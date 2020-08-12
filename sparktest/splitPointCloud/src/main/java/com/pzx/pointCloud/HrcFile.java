package com.pzx.pointCloud;

import com.pzx.IOUtils;
import scala.Tuple2;

import java.io.File;
import java.io.Serializable;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HrcFile implements Serializable {

    public static class NodeKeyComparator implements Comparator<String>,Serializable{
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
    }

    public static Comparator<String> nodeKeyComparator = new NodeKeyComparator();

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


    private static byte[] createHrcBytes(List<String> nodeKeyList){
        HashSet<String> nodeKeySet = (HashSet<String>) nodeKeyList.stream().collect(Collectors.toSet());

        nodeKeyList.sort(nodeKeyComparator);
        byte[] hrcBytes = new byte[nodeKeyList.size()];

        for(int i=0;i<hrcBytes.length;i++){
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

    /**
     *创建八叉树层次文件，并且在节点名后增加点数字节
     * @param outputDirPath 输出目录
     */
    public static void createHrcFileWithElementsNum(List<Tuple2<String, Integer>> nodeElementsTupleList, String outputDirPath){

        Map<String, Integer> nodeElementMap = nodeElementsTupleList.stream().collect(Collectors.groupingBy(Tuple2::_1, Collectors.reducing(0, Tuple2::_2, Integer::sum)));
        byte[] hrcBytes = createHrcBytesWithElementsNum(nodeElementMap);

        try {
            IOUtils.writerDataToFile(outputDirPath+File.separator+"r.hrc",hrcBytes,false);
        }catch (Exception e){
            e.printStackTrace();
        }

    }


    private static byte[] createHrcBytesWithElementsNum(Map<String, Integer> nodeElementsNumMap){
        List<String> nodeKeyList = nodeElementsNumMap.keySet().stream().collect(Collectors.toList());

        nodeKeyList.sort(nodeKeyComparator);

        byte[] hrcBytes = new byte[nodeKeyList.size()*2];

        for(int i=0;i<hrcBytes.length;i = i + 2){
            String nodeKey = nodeKeyList.get(i/2);
            byte mask = 0;

            for(int j=0;j<8;j++){
                if(nodeElementsNumMap.containsKey(nodeKey+j))
                    mask = (byte) (mask|1<<j);
            }
            hrcBytes[i] = mask;
            int elementsNum = nodeElementsNumMap.get(nodeKey);
            hrcBytes[i+1] = encodeElementsNum(elementsNum);
        }
        return hrcBytes;
    }

    /**
     * 将数量进行科学计数法编码，前四位表示指数，后四位表示底数，有精度缺失，最大可以表示15E15
     * @param elementsNum
     * @return
     */
    private static byte encodeElementsNum(int elementsNum){
        int index = String.valueOf(elementsNum).length()-1;
        return (byte) ((Math.round(elementsNum/Math.pow(10, index)))| index<<4);
    }

}
