# -*- encoding: utf-8 -*-
import pyspark
from splitUtils import getClod, getMaxLevel, getOctreeNodeKeyAndOffset,pointInZigZagFormat
import ioUtils
import os


def splitPointCloud(sc,pointList, cloudJS,outputFileDirPath,parallelism = 4):
    scale = cloudJS['scale']
    boundingBoxMax = [cloudJS['boundingBox']['ux'],cloudJS['boundingBox']['uy'],cloudJS['boundingBox']['uz']]
    boundingBoxmin = [cloudJS['boundingBox']['lx'],cloudJS['boundingBox']['ly'],cloudJS['boundingBox']['lz']]
    pointNum = cloudJS['points']
    maxLevel = getMaxLevel(pointNum,pointNumPerNode,dimension)
    #broadcatValue = sc.broadcast({"scale":scale,"boundingBoxMax":boundingBoxMax,"boundingBoxmin":boundingBoxmin,"maxLevel":maxLevel})
    print("maxlevel:--------------------"+ str(maxLevel))
    pointRdd = sc.parallelize(pointList, parallelism)

    pointRdd.map(lambda point: createNodeKeyPointValuePair(point,maxLevel,boundingBoxMax,boundingBoxmin,scale))\
        .groupByKey(numPartitions=parallelism)\
        .foreach(lambda nodeKeyPointListPair: writeNodeKeyPointListValuePair2File(nodeKeyPointListPair,outputFileDirPath))


def createNodeKeyPointValuePair(point,maxLevel,boundingBoxMax,boundingBoxmin,scale):
    clod = getClod(maxLevel,3)
    dlod = int(clod)
    nodekey,offset = getOctreeNodeKeyAndOffset(point,boundingBoxMax,boundingBoxmin,dlod)
    return nodekey,[int((point[i]-offset[i])/scale[i]) for i in range(len(offset))]


def writeNodeKeyPointListValuePair2File(nodeKeyPointListPair,outputFileDirPath):
    nodekey = nodeKeyPointListPair[0]
    pointList = nodeKeyPointListPair[1]
    nodeBytes = bytes()
    for point in pointList:
        nodeBytes = nodeBytes + pointInZigZagFormat(point)

    writer = ioUtils.writerDataToFile(outputFileDirPath + os.sep + nodekey + ".bin", mode="ab",myData=nodeBytes)



def initSpark(appName,master):
    sparkConf = pyspark.SparkConf().setAppName(appName).setMaster(master)
    sc = pyspark.SparkContext(conf=sparkConf)
    return sc


pointNumPerNode = 5000
dimension = 3





if __name__ == '__main__':


    pointList = [[128,2,4],[2,4,5],[42,0,0]]
    pointBytes = bytes()
    for point in pointList:
        print(pointInZigZagFormat(point))
        pointBytes += pointInZigZagFormat(point)
    with open("test.bin","wb") as writer:
        writer.write(pointBytes)



