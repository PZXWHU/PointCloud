package com.pzx.index.kdTree;

import com.pzx.geometry.Cuboid;
import com.pzx.geometry.MinimumBoundingBox;
import com.pzx.geometry.Point3D;
import com.pzx.index.SplittableTreeNode;
import org.codehaus.janino.IClass;


import java.io.Serializable;
import java.util.*;

public class KDBTreeNode<T extends MinimumBoundingBox> extends SplittableTreeNode<T> {

    private final int SPLIT_X = 1;
    private final int SPLIT_Y = 2;
    private final int SPLIT_Z = 3;

    private int splitDirection;
    private T middleElement;

    public KDBTreeNode(Cuboid region , int level , long maxElementsPerNode , int maxLevel){
       super(region, level, maxElementsPerNode, maxLevel);
    }

    @Override
    protected Cuboid[] splitRegion(Cuboid region) {

        splitDirection = region.getXSideLength() > region.getYSideLength() ?
                region.getXSideLength() > region.getZSideLength() ? SPLIT_X : SPLIT_Z :
                region.getYSideLength() > region.getZSideLength() ? SPLIT_Y : SPLIT_Z;


        Cuboid[] childrenCuboid = new Cuboid[2];
        Comparator<T> comparator = (element1, element2) ->{
            if (splitDirection == SPLIT_X)
                return Double.compare(element1.getMBB().getMinX(), element2.getMBB().getMinX());
            else if (splitDirection == SPLIT_Y)
                return Double.compare(element1.getMBB().getMinY(), element2.getMBB().getMinY());
            else
                return Double.compare(element1.getMBB().getMinZ(), element2.getMBB().getMinZ());
        };

        Collections.sort(elements, comparator);
        middleElement = elements.get((int) Math.floor(elements.size() / 2));

        switch (splitDirection){
            case SPLIT_X:
                childrenCuboid[0] = new Cuboid(region.getMinX(), region.getMinY(), region.getMinZ(), middleElement.getMBB().getMinX(), region.getMaxY(), region.getMaxZ());
                childrenCuboid[1] = new Cuboid(middleElement.getMBB().getMinX(), region.getMinY(), region.getMinZ(), region.getMaxX(), region.getMaxY(), region.getMaxZ());
                break;
            case SPLIT_Y:
                childrenCuboid[0] = new Cuboid(region.getMinX(), region.getMinY(), region.getMinZ(), region.getMaxX(), middleElement.getMBB().getMinY(), region.getMaxZ());
                childrenCuboid[1] = new Cuboid(region.getMinX(),  middleElement.getMBB().getMinY(), region.getMinZ(), region.getMaxX(), region.getMaxY(), region.getMaxZ());
                break;
            case SPLIT_Z:
                childrenCuboid[0] = new Cuboid(region.getMinX(), region.getMinY(), region.getMinZ(), region.getMaxX(), region.getMaxY(), middleElement.getMBB().getMinZ());
                childrenCuboid[1] = new Cuboid(region.getMinX(), region.getMinY(), middleElement.getMBB().getMinZ(), region.getMaxX(), region.getMaxY(), region.getMaxZ());
                break;
        }
        return childrenCuboid;
    }

    @Override
    protected SplittableTreeNode<T> createChildNode(Cuboid region) {
        return new KDBTreeNode<>(region, level + 1, maxElementsPerNode, maxLevel);
    }

    @Override
    /**
     * 以左下角点进行判断
     */
    protected SplittableTreeNode<T> queryChildNode(T element) {
        Point3D minPoint = element.getMBB().minPoint();
        int childIndex;
        if (region.contains(minPoint)){
            if (splitDirection ==SPLIT_X)
                childIndex =  minPoint.x <= middleElement.getMBB().getMinX() ? 0 : 1;
            else if (splitDirection == SPLIT_Y)
                childIndex =  minPoint.y <= middleElement.getMBB().getMinY() ? 0 : 1;
            else
                childIndex =  minPoint.z <= middleElement.getMBB().getMinZ() ? 0 : 1;

            return children.get(childIndex);
        }
        throw new IllegalArgumentException("无法找到包含输入要素的子节点");

    }
}
