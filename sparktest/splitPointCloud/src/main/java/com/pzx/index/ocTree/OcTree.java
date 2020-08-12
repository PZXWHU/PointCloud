package com.pzx.index.ocTree;

import com.pzx.geometry.Cuboid;
import com.pzx.geometry.MinimumBoundingBox;
import com.pzx.index.SplittableTree;

import java.io.Serializable;
import java.util.List;

public class OcTree<T extends MinimumBoundingBox> extends SplittableTree<T>  {

    public OcTree(Cuboid region , long maxElementsPerNode , int maxLevel){
        super(new OcTreeNode(region,0, maxElementsPerNode , maxLevel), region);
    }

    @Override
    public List<T> knnQuery(int k) {
        return null;
    }

    @Override
    public boolean remove(T element) {
        return false;
    }
}
