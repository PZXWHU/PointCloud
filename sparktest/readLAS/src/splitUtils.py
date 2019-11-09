# -*- encoding: utf-8 -*-
import numpy as np
import math


def getClod(maxLevel, dimension):
    random = np.random.rand()
    clod = (math.log((math.pow(2,dimension*maxLevel+dimension)-1)*random+1)/(dimension*math.log(2)))
    return clod


def getMaxLevel(pointNum, pointNumPerNode, dimension):
    blockNum = pointNum/pointNumPerNode
    maxLevel = int(math.ceil(math.log(blockNum)/math.log(math.pow(2,dimension))))
    return maxLevel


def getOctreeNodeKeyAndOffset(point, boundingBoxMax, boundingBoxMin, dlod):
    pointZIndex = ""
    pointMax = [boundingBoxMax[0], boundingBoxMax[1], boundingBoxMax[2]]
    pointMin = [boundingBoxMin[0], boundingBoxMin[1], boundingBoxMin[2]]
    offset = [boundingBoxMin[0], boundingBoxMin[1], boundingBoxMin[2]]
    while dlod>0:
        for i in range(3):
            mid = (pointMax[i]-pointMin[i])/2+pointMin[i]
            if(point[i]<mid):
                pointZIndex += "0"
                pointMax[i] = mid
            else:
                pointZIndex += "1"
                offset[i] += mid-pointMin[i]
                pointMin[i] = mid
        dlod = dlod-1
    nodekey = "r" + "".join([str(int(pointZIndex[i:i+3],2)) for i in range(len(pointZIndex)) if i%3==0])
    return nodekey,offset


'''低位在前'''
def pointInZigZagFormat(point):
    zigZagBytes = bytes()

    for i in range(3):
        x = point[i]
        while((x >> 7) > 0):

            zigZagBytes += bytes([(1 << 7)+ (x & 0x7f)])

            x = x >> 7
        zigZagBytes += bytes([x])
    return zigZagBytes


if __name__ == '__main__':
    point = [58098, 16397, 31411]
    print(pointInZigZagFormat(point))
