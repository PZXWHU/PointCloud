package com;

import com.pzx.SplitUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SplitUtilsTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getClod() {

        byte b = -27;

        System.out.println(Integer.toBinaryString(b&0x000000FF));

        /*
        for(int i=0;i<100000;i++){
            double clod = SplitUtils.getClod(4,3);
            if(clod>=5)
                System.out.println(clod);
        }


         */

    }

    @Test
    public void getMaxLevel() {

        int maxLevel = SplitUtils.getMaxLevel(97000000,30000l,2);
        System.out.println(maxLevel);

    }
    @Test
    public void getLocationOnSingleAxis(){

        //String name  = SplitUtils.getLocationOnSingleAxis(1.26,10,0,4);
        //System.out.println(name);
    }

    @Test
    public void getNodeName(){
        //String nodeName = SplitUtils.getNodeName("101","000","110");
        //System.out.println(nodeName);
    }

    @Test
    public void getOctreeNodeName(){
        String nodeName = SplitUtils.getOctreeNodeName(41,32,31,new double[]{123,324,532,0,0,0},9);
        System.out.println(nodeName);
    }

    @Test
    public void getXYZOffset(){
        double[] offsets = SplitUtils.getXYZOffset("r1004",new double[]{10,10,10,0,0,0});
        for (double offset :offsets){
            System.out.println(offset);
        }
    }

}