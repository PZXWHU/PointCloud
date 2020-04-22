package com.pzx.spatialPartition;

import com.pzx.geometry.Cuboid;
import com.pzx.geometry.Point3D;
import com.pzx.geometry.WithCuboidMBR;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class OcTreeNode<T extends WithCuboidMBR> implements Serializable {

    private Cuboid region;

    private List<OcTreeNode<T>> children;

    private List<T> elements = new ArrayList<>();

    private int level;

    public static final int REGION_SX_SY_SZ = Integer.parseInt("000",2);
    public static final int REGION_BX_SY_SZ = Integer.parseInt("100",2);
    public static final int REGION_SX_BY_SZ = Integer.parseInt("010",2);
    public static final int REGION_BX_BY_SZ = Integer.parseInt("110",2);
    public static final int REGION_SX_SY_BZ = Integer.parseInt("001",2);
    public static final int REGION_BX_SY_BZ = Integer.parseInt("101",2);
    public static final int REGION_SX_BY_BZ = Integer.parseInt("011",2);
    public static final int REGION_BX_BY_BZ = Integer.parseInt("111",2);

    private long maxElementsPerNode;

    private int maxLevel;

    private long elementNum = 0;

    public OcTreeNode(Cuboid region , long maxElementsPerNode , int maxLevel){
        this.region = region;
        this.level = 0; //根节点
        this.maxElementsPerNode = maxElementsPerNode;
        this.maxLevel = maxLevel;
    }

    public OcTreeNode(Cuboid region , int level , long maxElementsPerNode , int maxLevel){
        this.region = region;
        this.level = level;
        this.maxElementsPerNode = maxElementsPerNode;
        this.maxLevel = maxLevel;
    }

    public boolean isLeafNode(){
        return children == null;
    }

    public void insert(T element){

        elementNum++;
        if (isLeafNode()){
            elements.add(element);
            maySplit();
            return;
        }

        Cuboid elementMBR = element.getCuboidMBR();
        //用MBR中心点确定要素所属的子节点，以确保只会插入一个子节点中
        int childIndex = findChild(elementMBR.centerPoint());
        children.get(childIndex).insert(element);

    }

    /**
     * 找到包含节点范围包含输入点的子节点
     * @param point3D
     * @return
     */
    public int findChild(Point3D point3D){
        for (int i = 0 ; i < children.size() ; i++){
            if(children.get(i).getRegion().contains(point3D)){
                return i;
            }
        }
        throw new IllegalArgumentException("无法找到包含输入要素的子节点");
    }



    public void maySplit(){
        if(elements.size() >= maxElementsPerNode && level < maxLevel){
            children = new ArrayList<>(8);

            Cuboid[] childrenRegions = region.split();
            for (int i = 0 ; i<=7 ; i++){
                children.add(i , createChildNode(childrenRegions[i]));
            }

            //要素数量清零，重新计数
            elementNum = 0;
            for(T element : elements){
                insert(element);
            }
            elements.clear();

        }
    }

    private OcTreeNode<T> createChildNode(Cuboid region){
        return new OcTreeNode<T>(region , level+1 , maxElementsPerNode , maxLevel);
    }



    public Cuboid getRegion(){
        return this.region;
    }

    public long getElementNum(){return  this.elementNum;}

    public List<T> getElements(){return this.elements;}

    public int getLevel(){return this.level;}

    public List<OcTreeNode<T>> getChildren(){return this.children;}


    //访问者模式接口

    /**
     * 访问者模式
     * @param <U>
     */
    public static interface Visitor<U extends WithCuboidMBR>{
        /**
         * Visits a single node of the tree
         *
         * @param treeNode Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(OcTreeNode<U> treeNode);
    }

    /**
     * Traverses the tree top-down breadth-first and calls the visitor
     * for each node. Stops traversing if a call to visit returns false.
     */
    void traverse(Visitor<T> visitor)
    {
        if (!visitor.visit(this)) {
            return;
        }

        if (children != null) {
            for(OcTreeNode<T> child : children ){
                child.traverse(visitor);
            }
        }
    }

}
