package com;



public class SplitUtils {

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



    public static int getMaxLevel(long pointNum,long pointNumPerNode,int dimension){
        long blockNum = pointNum/pointNumPerNode;
        int maxLevel = (int)Math.ceil(Math.log(blockNum)/Math.log(Math.pow(2,dimension)));
        return maxLevel;
    }

    /**
     *
     * @param x
     * @param y
     * @param z
     * @param boundingBox maxx, maxy, maxz,minx,miny,minz
     * @param clod
     * @return
     */
    public static String getOctreeNodeName(double x,double y,double z,double[] boundingBox,double clod){
        int dlod = (int)clod;
        String xLocation = getLocationOnSingleAxis(x,boundingBox[0],boundingBox[3],dlod);
        String yLocation = getLocationOnSingleAxis(y,boundingBox[1],boundingBox[4],dlod);
        String zLocation = getLocationOnSingleAxis(z,boundingBox[2],boundingBox[5],dlod);

        return getNodeName(xLocation,yLocation,zLocation);

    }


    public static String getLocationOnSingleAxis(double x,double maxx,double minx,int dlod){
        String locationOnSingleAxis = "";
        /*
        if(dlod==0)
            return nodeName;

        double middlex = minx+(maxx-minx)/2;
        if(x<middlex)
            return "0"+getLocationOnSingleAxis(x,middlex,minx,dlod-1);
        else
            return "1"+getLocationOnSingleAxis(x,maxx,middlex,dlod-1);

         */

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

    public static String getNodeName(String xLocation,String yLocation,String zLocation){
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


    public static double[] getXYZOffset(String nodeKey,double[] boundingBox){
        int level = nodeKey.length()-1;
        double xOffset = 0.0;
        double yOffset = 0.0;
        double zOffset = 0.0;
        double boxLength = boundingBox[0]-boundingBox[3];
        for(int i=1;i<level+1;i++){
            int childNodeIndex = Integer.valueOf(nodeKey.substring(i,i+1));
            xOffset += Integer.valueOf(childNodeIndex>>2&1)*boxLength/Math.pow(2,i);
            yOffset += Integer.valueOf(childNodeIndex>>1&1)*boxLength/Math.pow(2,i);
            zOffset += Integer.valueOf(childNodeIndex&1)*boxLength/Math.pow(2,i);
        }
        return new double[]{xOffset,yOffset,zOffset};
    }




}
