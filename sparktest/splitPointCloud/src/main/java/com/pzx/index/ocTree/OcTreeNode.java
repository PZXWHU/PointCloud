package com.pzx.index.ocTree;

import com.pzx.geometry.Cuboid;
import com.pzx.geometry.MinimumBoundingBox;
import com.pzx.geometry.Point3D;
import com.pzx.index.SplittableTreeNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class OcTreeNode<T extends MinimumBoundingBox> extends SplittableTreeNode<T> {

    public OcTreeNode(Cuboid region , int level , long maxElementsPerNode , int maxLevel){
        super(region, level, maxElementsPerNode, maxLevel);
    }

    /**
     * 将cuboid分为八个子cuboid
     * @return
     */
    @Override
    public Cuboid[] splitRegion(Cuboid region){

        double childXLength = region.getXSideLength() / 2;
        double childYLength = region.getYSideLength() / 2;
        double childZLength = region.getZSideLength() / 2;

        Cuboid[] childrenCuboid = new Cuboid[8];
        for(int i=0 ; i<=7 ; i ++){
            double newMinX = region.getMinX() + childXLength * (i>>2 & 1);
            double newMinY = region.getMinY() + childYLength * (i>>1 & 1);
            double newMinZ = region.getMinZ() + childZLength * (i>>0 & 1);
            childrenCuboid[i] = new Cuboid(newMinX,newMinY,newMinZ,
                    newMinX + childXLength,newMinY + childYLength,newMinZ + childZLength);
        }

        return childrenCuboid;
    }

    @Override
    /**
     * 以左下角点进行判断
     */
    protected SplittableTreeNode<T> queryChildNode(T element) {
        Point3D minPoint = element.getMBB().minPoint();
        int childIndex = 0;
        if (region.contains(minPoint)){
            childIndex = (minPoint.x <= (region.getMinX() + region.getMaxX()) / 2) ? childIndex : childIndex | (1 << 2);
            childIndex = (minPoint.y <= (region.getMinY() + region.getMaxY()) / 2) ? childIndex : childIndex | (1 << 1);
            childIndex = (minPoint.z <= (region.getMinZ() + region.getMaxZ()) / 2) ? childIndex : childIndex | (1 << 0);
            return children.get(childIndex);
        }
        throw new IllegalArgumentException("无法找到包含输入要素的子节点");
    }


    @Override
    protected OcTreeNode<T> createChildNode(Cuboid region){
        return new OcTreeNode<T>(region , level+1 , maxElementsPerNode , maxLevel);
    }


}
