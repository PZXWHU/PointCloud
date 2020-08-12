package com.pzx.index.kdTree;

import com.pzx.geometry.Cuboid;
import com.pzx.geometry.MinimumBoundingBox;
import com.pzx.index.SplittableTree;
import com.pzx.index.SplittableTreeNode;

import java.io.Serializable;
import java.util.List;

public class KDBTree<T extends MinimumBoundingBox> extends SplittableTree<T> {

    public KDBTree(Cuboid region , long maxElementsPerNode , int maxLevel){
        super(new KDBTreeNode<>(region,0, maxElementsPerNode , maxLevel), region);
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
