package com.pzx;

public enum PointAttribute {

    //POSITION_CARTESIAN:坐标x,y,z int型
    //RGBA_PACKED：颜色r,g,b,a  byte型
    //RGB_PACKED：颜色r,g,b  byte型
    //Clod : float型
    POSITION_CARTESIAN(3*32),
    RGBA_PACKED(4*8),
    RGB_PACKED(3*8),
    INTENSITY(16),
    CLOD(32);

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
