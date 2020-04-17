package com.pzx.spatialPartition;

import com.pzx.geom.WithCuboidMBR;

/**
 * 访问者模式
 * @param <U>
 */
interface Visitor<U extends WithCuboidMBR>{
    /**
     * Visits a single node of the tree
     *
     * @param treeNode Node to visit
     * @return true to continue traversing the tree; false to stop
     */
    boolean visit(OcTreeNode<U> treeNode);
}
