package com.pzx.utils;



import com.pzx.geometry.Cube;
import com.pzx.geometry.Cuboid;
import com.pzx.geometry.Point3D;
import static com.pzx.pointCloud.PointCloud.*;
import org.apache.log4j.Logger;
import scala.Tuple2;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SplitUtils {

    private static Logger logger = Logger.getLogger(SparkUtils.class);

    private SplitUtils(){

    }

    public static double getClod(long pointNum,long pointNumPerNode,int dimension){

        int maxLevel = getMaxLevel(pointNum,pointNumPerNode,dimension);
        double random = Math.random();
        double clod =  Math.log((Math.pow(2,dimension*maxLevel+dimension)-1)*random+1)/(dimension*Math.log(2));

        return clod;
    }

    public static double getClod(int maxLevel,int dimension){

        double random = Math.random();
        double clod = (Math.log((Math.pow(2,dimension*maxLevel+dimension)-1)*random+1)/(dimension*Math.log(2)));
        return clod;
    }

    private static double getTheLevelClod(int maxLevel,int dimension,int level){
        double x1 = (Math.exp(level*dimension*Math.log(2))-1)/(Math.pow(2,dimension*maxLevel+dimension)-1);
        double x2 = (Math.exp((level+1)*dimension*Math.log(2))-1)/(Math.pow(2,dimension*maxLevel+dimension)-1);
        double random = Math.random()*(x2-x1)+x1;

        double clod =  Math.log((Math.pow(2,dimension*maxLevel+dimension)-1)*random+1)/(dimension*Math.log(2));
        return clod;
    }


    public static int getMaxLevel(long pointNum,long pointNumPerNode,int dimension){
        long blockNum = pointNum/pointNumPerNode;
        logger.info("-------------------------最大层级："+(Math.log(blockNum)/Math.log(Math.pow(2,dimension))));
        double doubleLevel = Math.log(blockNum)/Math.log(Math.pow(2,dimension));
        int maxLevel;
        //如果小数点后超过0.4 则向上取整，否则向下取整
        if(doubleLevel>=((int)doubleLevel+0.4)){
            maxLevel = (int)Math.ceil(doubleLevel);
        }else {
            maxLevel = (int)Math.round(doubleLevel);
        }
        return maxLevel;
    }

    /**
     *获取点所属的八叉树节点的名称
     * @param x
     * @param y
     * @param z
     * @param boundingBox maxx, maxy, maxz,minx,miny,minz
     * @param clod
     * @return
     */
    public static String getOctreeNodeName(double x, double y, double z, Cuboid boundingBox, double clod){
        int dlod = (int)clod;
        String xLocation = getLocationOnSingleAxis(x,boundingBox.getMaxX(),boundingBox.getMinX(),dlod);
        String yLocation = getLocationOnSingleAxis(y,boundingBox.getMaxY(),boundingBox.getMinY(),dlod);
        String zLocation = getLocationOnSingleAxis(z,boundingBox.getMaxZ(),boundingBox.getMinZ(),dlod);

        return getNodeName(xLocation,yLocation,zLocation);

    }

    public static String getOctreeNodeName(Point3D point3D,Cuboid boundingBox, double clod){
        return getOctreeNodeName(point3D.x, point3D.y, point3D.z, boundingBox, clod);
    }



    /**
     * 获取x在x轴上的位置，level为dlod
     * @param x
     * @param maxx
     * @param minx
     * @param dlod
     * @return 二进制字符串，表示x在x轴上的位置 二分法表示
     */
    private static String getLocationOnSingleAxis(double x,double maxx,double minx,int dlod){
        String locationOnSingleAxis = "";

        if(maxx<minx)
            throw new IllegalArgumentException("输入范围最大最小值颠倒！");

        while (dlod>0){
            double middlex = minx+(maxx-minx)/2;
            if(x<middlex){
                locationOnSingleAxis+="0";
                maxx = middlex;
            }
            else{
                locationOnSingleAxis+="1";
                minx = middlex;
            }
           dlod--;
        }
        return locationOnSingleAxis;
    }

    /**
     * 获取八叉树节点名字
     * @param xLocation 二进制字符串表示点在x轴上的位置
     * @param yLocation
     * @param zLocation
     * @return
     */
    private static String getNodeName(String xLocation,String yLocation,String zLocation){
        if(xLocation.length()==yLocation.length()?(xLocation.length()==zLocation.length()?false:true):true)
            throw new IllegalArgumentException("三个字符串参数长度不一致！");
        String nodeName = "r";
        int length = xLocation.length();
        for(int i=0;i<length;i++){
            String locationStr = ""+xLocation.charAt(i)+yLocation.charAt(i)+zLocation.charAt(i);
            nodeName += "" +Integer.parseInt(locationStr,2);
        }
        return nodeName;
    }


    /**
     * 获取某个八叉树节点左下角的offset
     * @param nodeKey r0123
     * @param boundingBox
     * @return
     */
    public static double[] getXYZOffset(String nodeKey, Cube boundingBox){
        int level = nodeKey.length()-1;
        double xOffset = 0.0;
        double yOffset = 0.0;
        double zOffset = 0.0;
        double boxLength = boundingBox.getSideLength();
        for(int i=1;i<level+1;i++){
            int childNodeIndex = Integer.valueOf(nodeKey.substring(i,i+1));
            xOffset += Integer.valueOf(childNodeIndex>>2&1)*boxLength/Math.pow(2,i);
            yOffset += Integer.valueOf(childNodeIndex>>1&1)*boxLength/Math.pow(2,i);
            zOffset += Integer.valueOf(childNodeIndex&1)*boxLength/Math.pow(2,i);
        }
        return new double[]{xOffset+boundingBox.getMinX(),yOffset+boundingBox.getMinY(),zOffset+boundingBox.getMinZ()};
    }

    public static Cube getNodeBoundingBox(String nodeKey, Cube totalBoundingBox){
        double[] xyzOffset = getXYZOffset(nodeKey, totalBoundingBox);
        double nodeSideLength = totalBoundingBox.getSideLength()/(1<<(nodeKey.length()-1));

        Cube nodeBoundingBox = new Cube(xyzOffset[0], xyzOffset[1], xyzOffset[2], nodeSideLength);

        return nodeBoundingBox;
    }

    /**
     * 以ZigZagFormat编码XYZ
     * @param xyzCoordinates
     * @return
     */
    public static byte[] pointInZigZagFormat(int[] xyzCoordinates){
        List<Byte> list = new ArrayList<>();
        for(int coordinate :xyzCoordinates){
            while (coordinate>>7 > 0){
                list.add((byte)((1 << 7)+ (coordinate & 0x7f)));
                coordinate = coordinate >>7;
            }
            list.add((byte)coordinate);
        }
        byte[] zigZagBytes = new byte[list.size()];
        for(int i =0;i<list.size();i++){
            zigZagBytes[i] = list.get(i);
        }
        return zigZagBytes;
    }

    public static String createBinFileName(String nodeKey){
        return (nodeKey.length()-1)+nodeKey+".bin";
    }

    public static String createHrcFileName(){
        return "r.hrc";
    }

    public static String createCloudJSFileName(){
        return "cloud.js";
    }

    public static  <T, U> List<Tuple2<T, U>> mapToTupleList(Map<T, U> map){
        List<Tuple2<T, U>> tupleList = map.entrySet().stream()
                .map(entry->new Tuple2<T, U>(entry.getKey(), entry.getValue())).collect(Collectors.toList());
        return tupleList;
    }

    public static void main(String[] args) {
        System.out.println(getTheLevelClod(8,2,0));
    }

}
