package com.pzx.index;

import com.pzx.geometry.Cuboid;
import com.pzx.geometry.MinimumBoundingBox;
import org.apache.commons.lang3.mutable.MutableInt;

import java.io.Serializable;
import java.util.*;

/**
 * 可分裂的树
 * @param <T>
 */
public abstract class SplittableTree<T extends MinimumBoundingBox> implements Index<T>, Serializable {

    protected SplittableTreeNode<T> root;
    protected Cuboid region;

    public SplittableTree(SplittableTreeNode<T> root, Cuboid region) {
        this.root = root;
        this.region = region;
    }

    @Override
    /**
     * 范围相交查询，利用元素的最小包围盒判断相交，所以为粗查询
     */
    public List<T> rangeQuery(Cuboid cuboid) {
        List<T> resultElements = new ArrayList<>();
        root.traverse(new SplittableTreeNode.Visitor<T>() {
            @Override
            public boolean visit(SplittableTreeNode<T> treeNode) {
                if(treeNode.getRegion().disjoint(cuboid))
                    return false;
                if(treeNode.isLeafNode()){
                    for(T element : treeNode.getElements()){
                        if (cuboid.intersects(element)){
                            resultElements.add(element);
                        }
                    }
                }
                return true;
            }
        });
        return resultElements;
    }

    @Override
    public final boolean remove(T element) {
        return false;
    }

    @Override
    public final void insert(T element) {
        root.insert(element);
    }

    /**
     * 获得范围相交的叶子节点范围
     * @param region
     * @return
     */
    public final <U extends MinimumBoundingBox> List<Cuboid> queryLeafNodeRegions(U region){
        List<Cuboid> resultRegions = new ArrayList<>();
        root.traverse(new SplittableTreeNode.Visitor<T>() {
            @Override
            public boolean visit(SplittableTreeNode<T> treeNode) {
                if(treeNode.getRegion().disjoint(region)){
                    return false;
                }
                if(treeNode.isLeafNode()){
                    resultRegions.add(treeNode.getRegion());
                }
                return true;
            }
        });
        return resultRegions;
    }

    /**
     * 获取所有八叉树叶子节点的范围
     * @return
     */
    public final List<Cuboid> getAllLeafNodeRegions(){
        List<Cuboid> leafRegions = new ArrayList<>();
        root.traverse(new SplittableTreeNode.Visitor<T>() {
            @Override
            public boolean visit(SplittableTreeNode<T> treeNode) {
                if(treeNode.isLeafNode()){
                    leafRegions.add(treeNode.getRegion());
                }
                return true;
            }
        });
        return leafRegions;
    }

    /*
    public final List<Long> getLeafNodeElementsNums(){
        List<Long> leafElementsNum = new ArrayList<>();
        root.traverse(new SplittableTreeNode.Visitor<T>() {
            @Override
            public boolean visit(SplittableTreeNode<T> treeNode) {
                if(treeNode.isLeafNode()){
                    leafElementsNum.add(treeNode.getElementNum());
                }
                return true;
            }
        });
        return leafElementsNum;
    }

    public final Map<Cuboid, Long> getLeafNodeRegionsAndElementsNums() {
        Map<Cuboid, Long> leafRegionAndElementsNum = new HashMap<>();
        root.traverse(new SplittableTreeNode.Visitor<T>() {
            @Override
            public boolean visit(SplittableTreeNode<T> treeNode) {
                if (treeNode.isLeafNode()) {
                    leafRegionAndElementsNum.put(treeNode.getRegion(), treeNode.getElementNum());
                }
                return true;
            }
        });
        return leafRegionAndElementsNum;
    }
     */

    /**
     * 获得树中插入的所有要素的总数目
     * @return
     */
    public final long getTotalElementNum() {
        return root.getElementNum();
    }

    public final int getTreeLevel(){
        MutableInt level = new MutableInt(0);
        root.traverse(new SplittableTreeNode.Visitor<T>() {
            @Override
            public boolean visit(SplittableTreeNode<T> treeNode) {
                if(treeNode.isLeafNode()) {
                    level.setValue(Math.max(level.getValue(), treeNode.getLevel()));
                }
                return true;
            }
        });
        return level.getValue();
    }

    public final void clearAllElements(){
        root.traverse(new SplittableTreeNode.Visitor<T>() {
            @Override
            public boolean visit(SplittableTreeNode<T> treeNode) {
                treeNode.elements = null;
                return true;
            }
        });
    }

    public final Cuboid getRegion(){return region;}

    public final void printTree(){
        root.traverse(new SplittableTreeNode.Visitor<T>() {
            @Override
            public boolean visit(SplittableTreeNode<T> treeNode) {
                if(treeNode.isLeafNode()) {
                    System.out.println("" + treeNode.getRegion() + treeNode.getElementNum());
                }
                return true;
            }
        });

    }

}
