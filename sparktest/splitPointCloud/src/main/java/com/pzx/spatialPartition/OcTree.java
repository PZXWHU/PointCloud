package com.pzx.spatialPartition;

import com.pzx.geom.Cuboid;
import com.pzx.geom.Point3D;
import com.pzx.geom.WithCuboidMBR;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

import javax.swing.text.DefaultEditorKit;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class OcTree<T extends WithCuboidMBR> implements Serializable {

    private OcTreeNode<T> root;
    private int treeLevel;

    public OcTree(Cuboid region , long maxElementsPerNode , int maxLevel){
        this.root = new OcTreeNode(region , maxElementsPerNode , maxLevel);

    }

    public void insert(T element){
        root.insert(element);
    }

    public List<T> queryContains(Cuboid cuboid){
        List<T> resultElements = new ArrayList<>();
        root.traverse(new Visitor<T>() {
            @Override
            public boolean visit(OcTreeNode<T> treeNode) {
                if(!treeNode.getRegion().intersects(cuboid))
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
        root.traverse(new Visitor<T>() {
            @Override
            public boolean visit(OcTreeNode<T> treeNode) {
                if(!treeNode.getRegion().intersects(cuboid))
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
     * 获取八叉树叶子节点的范围
     * @return
     */
    public List<Cuboid> getAllLeafNodeRegion(){
        List<Cuboid> leafRegions = new ArrayList<>();
        root.traverse(new Visitor<T>() {
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

    /**
     * 获得树中插入的所有要素的总数目
     * @return
     */
    public long getTotalElementNum()
    {
        return root.getElementNum();
        /*
        final MutableLong elementCount = new MutableLong(0);

        root.traverse(new Visitor<T>()
        {
            @Override
            public boolean visit(OcTreeNode<T> treeNode)
            {
                if (treeNode.isLeafNode()) {
                    elementCount.add(treeNode.getElementNum());
                }
                return true;
            }
        });
        return elementCount.getValue();

         */
    }

    public int getTreeLevel(){
        MutableInt level = new MutableInt(0);
        root.traverse(new Visitor<T>() {
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

    public OcTreeNode<T> getRootTreeNode(){return this.root;}

    public void printTree(){

        root.traverse(new Visitor<T>() {
            @Override
            public boolean visit(OcTreeNode<T> treeNode) {
                if(treeNode.isLeafNode()) {
                    System.out.println("" + treeNode.getRegion() + treeNode.getElementNum());
                }
                return true;
            }
        });

    }

    public static void main(String[] args) {
        OcTree<Point3D> ocTree = new OcTree<>(new Cuboid(0,0,0,100,100,100),10000,40);
        long time = System.currentTimeMillis();
        for(int i = 0; i<10000000 ; i++){

            Point3D point3D = new Point3D(Math.random()*100,Math.random()*100,Math.random()*100);
            ocTree.insert(point3D);

        }
        System.out.println(System.currentTimeMillis() - time);
        time = System.currentTimeMillis();
        System.out.println(ocTree.queryContains(new Cuboid(0,0,0,10,10,10)));
        System.out.println(System.currentTimeMillis() - time);



    }





}
