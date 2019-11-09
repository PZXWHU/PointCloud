# encoding: utf-8
import json
import os
import numpy as np
import gc
import sys
import time
from sparkTask import splitPointCloud
import functools
import lasReader
import ioUtils
import sparkTask


def dict2file(dic, outputFileDirPath):
    '''生成或者重写cloud.js'''
    writer = ioUtils.writerDataToFile(outputFileDirPath + os.sep + "cloud.js",myData=json.dumps(dic),mode="w")


def doSparkTask(sc, pointList, cloudJS,outputFileDirPath,parallelism):
    splitPointCloud(sc,pointList,cloudJS,outputFileDirPath,parallelism)


def processDic(lasFileDirPath, pointBuffer):
    lasFileNameList = os.listdir(lasFileDirPath)
    for lasFileName in lasFileNameList:
        if(os.path.isdir(lasFileDirPath+os.sep+lasFileName)):
            pointBuffer = processDic(lasFileDirPath+os.sep+lasFileName, pointBuffer)
            continue
        if(not lasFileName.endswith(".las")):
            continue
        print(lasFileName)

        header, points = lasReader.readLASFile(lasFileDirPath + os.sep + lasFileName)
        pointNum = header.count

        if(len(pointBuffer)+pointNum > pointBufferLimit):
            doSparkTask(sc, pointBuffer, cloudJS,outputFileDirPath,parallelism)
            del pointBuffer
            gc.collect()
            pointBuffer = np.empty((0, 3))

        pointBuffer = np.concatenate((pointBuffer,points), axis=0)
        del points
        del header
        gc.collect()
    return pointBuffer


def createHrcFile(outputFileDirPath):
    fileList = ioUtils.listDir(outputFileDirPath)
    nodeKeyList = [fileName[:-4] for fileName in fileList if fileName.endswith(".bin")]
    nodeKeyList.sort(key=functools.cmp_to_key(nodeKeySort))
    hrcBytes = bytes()
    for nodeKey in  nodeKeyList:
        mask = 0
        for i in range(8):
            if(nodeKey+str(i) in nodeKeyList):
                mask |=1<<i
        hrcBytes += bytes([mask])
    ioUtils.writerDataToFile(outputFileDirPath+os.sep+"r.hrc",myData=hrcBytes,mode="wb")


def nodeKeySort(x,y):
    if(len(x)<len(y)):
        return -1
    elif(len(x)>len(y)):
        return 1
    else:
        if(x>y):
            return 1
        elif (x<y):
            return -1
        else:
            return 0



if __name__ == '__main__':

    inputFileDirPath = "D:/wokspace/点云的储存与可视化/大数据集与工具/data/las"
    outputFileDirPath = "D:/wokspace/点云的储存与可视化/大数据集与工具/data/python"
    sparkMaster = "spark://master:7077"
    if(len(sys.argv)>2):
        inputFileDirPath = sys.argv[1]
        outputFileDirPath = sys.argv[2]
    if(len(sys.argv)>3):
        sparkMaster = sys.argv[3]

    pointBufferLimit = 10000000
    parallelism = 24

    # 获取点云信息，生成cloudjs文件
    cloudJS = lasReader.getPointCloudInformation(inputFileDirPath)
    dict2file(cloudJS, outputFileDirPath)

    # 流式读取las文件，一千万个点触发一次spark分片
    sc = sparkTask.initSpark("splitPointCloud",sparkMaster)
    pointBuffer = np.empty((0, 3))

    inputFileDirPaths = [inputFileDirPath]
    for lasFileDirPath in inputFileDirPaths:
        pointBuffer = processDic(lasFileDirPath,pointBuffer)
    doSparkTask(sc, pointBuffer, cloudJS,outputFileDirPath,parallelism)

    createHrcFile(outputFileDirPath)