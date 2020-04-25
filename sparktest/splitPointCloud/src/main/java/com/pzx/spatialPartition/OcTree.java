package com.pzx.spatialPartition;

import com.pzx.geometry.Cuboid;
import com.pzx.geometry.Point3D;
import com.pzx.geometry.WithCuboidMBR;
import org.apache.commons.lang3.mutable.MutableInt;

import java.io.Serializable;
import java.util.*;

public class OcTree<T extends WithCuboidMBR> implements Serializable {

    private OcTreeNode<T> root;
    private Cuboid region;
    private int treeLevel;

    public OcTree(Cuboid region , long maxElementsPerNode , int maxLevel){
        this.root = new OcTreeNode(region , maxElementsPerNode , maxLevel);
        this.region = region;

    }

    public void insert(T element){
        root.insert(element);
    }

    public void insert(Iterator<T> elements){
        while (elements.hasNext())
            root.insert(elements.next());
    }

    /*-----------------------------------------------------------*/
    //query

    public List<T> queryContains(Cuboid cuboid){
        List<T> resultElements = new ArrayList<>();
        root.traverse(new OcTreeNode.Visitor<T>() {
            @Override
            public boolean visit(OcTreeNode<T> treeNode) {
                if(treeNode.getRegion().disjoint(cuboid))
                    return false;
                if(treeNode.isLeafNode()){
                    for(T element : treeNode.getElements()){
                        if (cuboid.contains(element)){
                            resultElements.add(element);
                        }
                    }
                }
                return true;
            }
        });
        return resultElements;
    }

    public List<T> queryIntersects(Cuboid cuboid){
        List<T> resultElements = new ArrayList<>();
        root.traverse(new OcTreeNode.Visitor<T>() {
            @Override
            public boolean visit(OcTreeNode<T> treeNode) {
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

    /**
     * 获得范围相交的叶子节点
     * @param region
     * @return
     */
    public <U extends WithCuboidMBR> List<Cuboid> findLeafNodeRegion(U region){
        List<Cuboid> resultRegions = new ArrayList<>();
        root.traverse(new OcTreeNode.Visitor<T>() {
            @Override
            public boolean visit(OcTreeNode<T> treeNode) {
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


    /*-----------------------------------------------------------*/
    //遍历获得叶节点信息

    /**
     * 获取所有八叉树叶子节点的范围
     * @return
     */
    public List<Cuboid> getLeafNodeRegions(){
        List<Cuboid> leafRegions = new ArrayList<>();
        root.traverse(new OcTreeNode.Visitor<T>() {
            @Override
            public boolean visit(OcTreeNode<T> treeNode) {
                if(treeNode.isLeafNode()){
                    leafRegions.add(treeNode.getRegion());
                }
                return true;
            }
        });
        return leafRegions;
    }

    public List<Long> getLeafNodeElementsNums(){
        List<Long> leafElementsNum = new ArrayList<>();
        root.traverse(new OcTreeNode.Visitor<T>() {
            @Override
            public boolean visit(OcTreeNode<T> treeNode) {
                if(treeNode.isLeafNode()){
                    leafElementsNum.add(treeNode.getElementNum());
                }
                return true;
            }
        });
        return leafElementsNum;
    }

    public Map<Cuboid, Long> getLeafNodeRegionsAndElementsNums(){
        Map<Cuboid, Long> leafRegionAndElementsNum = new HashMap();
        root.traverse(new OcTreeNode.Visitor<T>() {
            @Override
            public boolean visit(OcTreeNode<T> treeNode) {
                if(treeNode.isLeafNode()){
                    leafRegionAndElementsNum.put(treeNode.getRegion(), treeNode.getElementNum());
                }
                return true;
            }
        });
        return leafRegionAndElementsNum;
    }


    /*-----------------------------------------------------------*/
    //获得整个八叉树的信息

    /**
     * 获得树中插入的所有要素的总数目
     * @return
     */
    public long getTotalElementNum() {
        return root.getElementNum();
    }

    public int getTreeLevel(){
        MutableInt level = new MutableInt(0);
        root.traverse(new OcTreeNode.Visitor<T>() {
            @Override
            public boolean visit(OcTreeNode<T> treeNode) {
                if(treeNode.isLeafNode()) {
                    level.setValue(Math.max(level.getValue(), treeNode.getLevel()));
                }
                return true;
            }
        });
        return level.getValue();
    }

    /*-----------------------------------------------------------*/

    public OcTreeNode<T> getRootTreeNode(){return this.root;}

    public Cuboid getRegion(){return region;}

    public void printTree(){

        root.traverse(new OcTreeNode.Visitor<T>() {
            @Override
            public boolean visit(OcTreeNode<T> treeNode) {
                if(treeNode.isLeafNode()) {
                    System.out.println("" + treeNode.getRegion() + treeNode.getElementNum());
                }
                return true;
            }
        });

    }

    /*-----------------------------------------------------------*/


    public static void main(String[] args) {
        OcTree<Point3D> ocTree = new OcTree<>(new Cuboid(0,0,0,100,100,100),1000,40);
        long time = System.currentTimeMillis();
        for(int i = 0; i<1000000 ; i++){

            Point3D point3D = new Point3D(Math.random()*10,Math.random()*10,Math.random()*90);
            ocTree.insert(point3D);

        }
        ocTree.printTree();
        System.out.println(ocTree.getLeafNodeRegionsAndElementsNums());
    }





}
