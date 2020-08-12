package com.pzx.index;

import com.pzx.geometry.Cuboid;
import com.pzx.geometry.MinimumBoundingBox;

import java.util.List;

public interface Index<T extends MinimumBoundingBox> {

    void insert(T element);

    boolean remove(T element);

    List<T> rangeQuery(Cuboid cuboid);

    List<T> knnQuery(int k);

}
