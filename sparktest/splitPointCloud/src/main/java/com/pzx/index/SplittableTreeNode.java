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

    abstract protected Cuboid[] splitRegion(Cuboid region);

    abstract protected int queryChildIndex(T element);

    abstract protected SplittableTreeNode<T> createChildNode(Cuboid region);

    public final void insert(T element){

        elementNum++;
        if (isLeafNode()){
            elements.add(element);
            if(elements.size() >= maxElementsPerNode && level < maxLevel){
                splitNode();
            }
            return;
        }

        int childIndex = queryChildIndex(element);
        children.get(childIndex).insert(element);
    }

    private void splitNode(){
        Cuboid[] childrenRegions = splitRegion(region);
        children = new ArrayList<>(childrenRegions.length);
        for (int i = 0 ; i < childrenRegions.length ; i++){
            children.add(createChildNode(childrenRegions[i]));
        }
        for(T element : elements){
            children.get(queryChildIndex(element)).insert(element);
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
