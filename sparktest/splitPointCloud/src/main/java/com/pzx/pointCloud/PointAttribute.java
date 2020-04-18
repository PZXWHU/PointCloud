package com.pzx.pointCloud;

import java.io.Serializable;

public enum PointAttribute implements Serializable {

    //POSITION_CARTESIAN:坐标x,y,z int型
    //RGBA_PACKED：颜色r,g,b,a  byte型
    //RGB_PACKED：颜色r,g,b  byte型
    //Clod : float型
    POSITION_XYZ(3*4),
    RGB(3),
    LEVEL(1);

    //属性所占字节数
    private int bytesCount;
    private PointAttribute(int bytesCount){
        this.bytesCount = bytesCount;
    }

    /**
     *
     * @return 返回属性所占的字节数
     */
    public int getBytesCount(){
        return bytesCount;
    }


}
