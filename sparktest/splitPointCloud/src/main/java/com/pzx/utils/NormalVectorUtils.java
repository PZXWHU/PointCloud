package com.pzx.utils;

import com.google.common.base.Preconditions;
import com.pzx.geometry.Point3D;
import com.sun.org.apache.regexp.internal.RE;
import org.apache.commons.math3.linear.EigenDecomposition;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.correlation.Covariance;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class NormalVectorUtils {

    public static double[] getNormalVector(double[][] pointCoordinates){
        Preconditions.checkNotNull(pointCoordinates);
        Preconditions.checkArgument(pointCoordinates[0].length == 3, "point Coordinates must be three dimension");

        RealMatrix matrix = MatrixUtils.createRealMatrix(pointCoordinates);
        Covariance covariance = new Covariance(matrix);//求协方差矩阵
        RealMatrix covarianceMatrix = covariance.getCovarianceMatrix();//求协方差矩阵的特征值以及特征向量
        EigenDecomposition ed = new EigenDecomposition(covarianceMatrix);

        return ed.getEigenvector(ed.getRealEigenvalues().length - 1).toArray();//最小的特征值对应的特征向量就是法向量
    }

    public static double[] getNormalVector(List<Point3D> pointList, int k) {

        if (pointList.size() < 3)
            return new double[]{0,0,0};

        /*
        Collections.sort(pointList, (point1, point2)-> {
            return Double.compare(point3D.distance(point1), point3D.distance(point2));
        });
         */

        double[][] pointCoordinates = new double[k][3];

        int interval = pointList.size() / k;
        for (int i = 0; i < k; i++){
            Point3D p = pointList.get(i * interval);
            pointCoordinates[i] = new double[]{p.x, p.y, p.z};
        }
        return getNormalVector(pointCoordinates);
    }

}
