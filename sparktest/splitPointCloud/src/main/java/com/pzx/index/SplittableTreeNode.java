package com.pzx.index;

import com.pzx.geometry.Cuboid;
import com.pzx.geometry.MinimumBoundingBox;
import com.pzx.geometry.Point3D;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class SplittableTreeNode<T extends MinimumBoundingBox> implements Serializable {

    protected Cuboid region;

    protected List<SplittableTreeNode<T>> children;

    protected List<T> elements;

    protected int level;

    protected long maxElementsPerNode;

    protected int maxLevel;

    protected long elementNum = 0;//以该节点为根节点的子树含有的元素数量

    public SplittableTreeNode(Cuboid region , int level , long maxElementsPerNode , int maxLevel){
        this.region = region;
        this.level = level;
        this.maxElementsPerNode = maxElementsPerNode;
        this.maxLevel = maxLevel;
        this.elements = new ArrayList<>();
    }

    /**
     * 将节点范围划分为若个个子节点范围
     * @param region
     * @return
     */
    abstract protected Cuboid[] splitRegion(Cuboid region);

    /**
     * 查找当前节点中的元素所应该划分到的子节点（元素只会属于一个子节点，按照元素最小包围盒最下角判断）
     * @param element
     * @return
     */
    abstract protected SplittableTreeNode<T> queryChildNode(T element);

    abstract protected SplittableTreeNode<T> createChildNode(Cuboid region);

    /**
     * 插入导致分裂，当前节点中的元素只会插入到一个子节点
     * @param element
     */
    public final void insert(T element){

        elementNum++;
        if (isLeafNode()){
            elements.add(element);
            if(elements.size() > maxElementsPerNode && level < maxLevel){
                splitNode();
            }
            return;
        }

        queryChildNode(element).insert(element);
    }

    private void splitNode(){
        Cuboid[] childrenRegions = splitRegion(region);
        children = new ArrayList<>(childrenRegions.length);
        for (int i = 0 ; i < childrenRegions.length ; i++){
            children.add(createChildNode(childrenRegions[i]));
        }
        for(T element : elements){
            queryChildNode(element).insert(element);
        }
        elements = null;
    }


    public final boolean isLeafNode(){
        return children == null;
    }

    public final long getElementNum(){return  this.elementNum;}

    public final List<T> getElements(){return this.elements;}

    public final int getLevel(){return this.level;}

    public final Cuboid getRegion(){ return this.region; }

    public final List<SplittableTreeNode<T>> getChildren(){ return this.children; }

    //访问者模式接口

    /**
     * 访问者模式
     * @param <U>
     */
    public static interface Visitor<U extends MinimumBoundingBox>{
        /**
         * Visits a single node of the tree
         *
         * @param treeNode Node to visit
         * @return true to continue traversing the tree; false to stop
         */
        boolean visit(SplittableTreeNode<U> treeNode);
    }

    /**
     * Traverses the tree top-down breadth-first and calls the visitor
     * for each node. Stops traversing if a call to visit returns false.
     */
    public final void traverse(Visitor<T> visitor)
    {
        if (!visitor.visit(this)) {
            return;
        }
        if (children != null) {
            for(SplittableTreeNode<T> child : children ){
                child.traverse(visitor);
            }
        }
    }



}
