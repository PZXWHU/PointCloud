# -*- encoding: utf-8 -*-
import laspy
import os
import math
import time
import numpy as np
import gc




def getPointCloudInformation(*lasFileDirPaths):
    pointCount = 0
    pointTightBoundingMax = [float('-inf'), float('-inf'), float('-inf')]
    pointTightBoundingMin = [float('inf'), float('inf'), float('inf')]
    pointBoundingMax = [float('-inf'), float('-inf'), float('-inf')]
    pointBoundingMin = [float('inf'), float('inf'), float('inf')]
    scale = [1,1,1]

    for lasFileDirPath in lasFileDirPaths:
        lasFileList = os.listdir(lasFileDirPath)

        for lasFileName in lasFileList:
            '''如果是文件夹,递归'''
            if(os.path.isdir(lasFileDirPath+os.sep+lasFileName)):
                cloudjs = getPointCloudInformation(lasFileDirPath+os.sep+lasFileName)
                pointCount +=cloudjs['points']
                pointTightBoundingMax[0] = max(pointTightBoundingMax[0],cloudjs['tightBoundingBox']['ux'])
                pointTightBoundingMax[1] = max(pointTightBoundingMax[1],cloudjs['tightBoundingBox']['uy'])
                pointTightBoundingMax[2] = max(pointTightBoundingMax[2],cloudjs['tightBoundingBox']['uz'])
                pointTightBoundingMin[0] = min(pointTightBoundingMin[0],cloudjs['tightBoundingBox']['lx'])
                pointTightBoundingMin[1] = min(pointTightBoundingMin[1],cloudjs['tightBoundingBox']['ly'])
                pointTightBoundingMin[2] = min(pointTightBoundingMin[2],cloudjs['tightBoundingBox']['lz'])
                scale = [min(scale[i],cloudjs['scale'][i]) for i in range(len(scale))]
                continue
            '''如果不是las文件，不读取'''
            if(not lasFileName.endswith(".las")):
                continue
            '''las文件，读取'''
            f = laspy.file.File(lasFileDirPath+os.sep+lasFileName,mode="r")
            header = f.header

            scale = [min(scale[0], header.scale[0]), min(scale[1], header.scale[1]), min(scale[2], header.scale[2])]
            pointCount += header.count
            for i in range(len(pointTightBoundingMax)):
                pointTightBoundingMax[i] = max(pointTightBoundingMax[i],header.max[i])
            for i in range(len(pointTightBoundingMin)):
                pointTightBoundingMin[i] = min(pointTightBoundingMin[i],header.min[i])
            f.close()

        '''根据pointTightBounding获取pointBounding'''
        sideLength = float('-inf')
        for i in range(len(pointTightBoundingMax)):
            sideLength = max(sideLength, pointTightBoundingMax[i]-pointTightBoundingMin[i])
        for i in range(len(pointBoundingMax)):
            pointBoundingMin[i] = pointTightBoundingMin[i]
            pointBoundingMax[i] = sideLength + pointBoundingMin[i]

    cloudJS = {}
    cloudJS.update({
        "points": pointCount,
        "boundingBox": {
            "lx": pointBoundingMin[0],
            "ly": pointBoundingMin[1],
            "lz": pointBoundingMin[2],
            "ux": pointBoundingMax[0],
            "uy": pointBoundingMax[1],
            "uz": pointBoundingMax[2],
        },
        "tightBoundingBox": {
            "lx": pointTightBoundingMin[0],
            "ly": pointTightBoundingMin[1],
            "lz": pointTightBoundingMin[2],
            "ux": pointTightBoundingMax[0],
            "uy": pointTightBoundingMax[1],
            "uz": pointTightBoundingMax[2],
        },
        "scale": scale,
        "space": math.pow(math.pow(pointBoundingMax[0]-pointBoundingMin[0],2)+math.pow(pointBoundingMax[1]-pointBoundingMin[1],2)+math.pow(pointBoundingMax[2]-pointBoundingMin[2],2),1.0/3)/250,
        "pointAttributes": ["POSITION_CARTESIAN"]
    })
    return cloudJS


def readLASFilePointData(lasFilePath):
    lasFile = laspy.file.File(lasFilePath, mode="r")
    header = lasFile.header
    scale = header.scale
    offset = header.offset
    X = lasFile.X*scale[0]+offset[0]
    Y = lasFile.Y*scale[1]+offset[1]
    Z = lasFile.Z*scale[2]+offset[2]
    pointData = np.array([X,Y,Z]).T

    # pointItem = ((5763582, 3079752, 125509, 0, 9, 0, 0, 0, 0),)
    # 使用长列表太耗时，应该使用numpy进行处理
    # pointData = [[pointItem[0][i]*scale[i]+offset[i] for i in range(3)] for pointItem in lasFile.points]

    lasFile.close()
    return  pointData


def readLASFileHeader(lasFilePath):
    lasFile = laspy.file.File(lasFilePath, mode="r")
    header = lasFile.header
    lasFile.close()
    return header


def readLASFile(lasFilePath):
    lasFile = laspy.file.File(lasFilePath, mode="r")
    header = lasFile.header
    scale = header.scale
    offset = header.offset
    X = lasFile.X*scale[0]+offset[0]
    Y = lasFile.Y*scale[1]+offset[1]
    Z = lasFile.Z*scale[2]+offset[2]
    pointData = np.array([X,Y,Z]).T
    lasFile.close()
    return header,pointData

if __name__ == '__main__':
    lasFilePath = "D:/wokspace/点云的储存与可视化/大数据集与工具/data/las"
    '''
    lasFile = laspy.file.File(lasFilePath, mode="r")
    header = lasFile.header
    lasFile.close()
    print(header.count)
    '''

    lasFileList = os.listdir("D:/wokspace/点云的储存与可视化/大数据集与工具/data/las")
    pointBuffer = []
    for lasFileName in lasFileList:
        print(lasFileName)
        ticks = time.time()
        #cloudjs = getPointCloudInformation("D:/wokspace/点云的储存与可视化/大数据集与工具/data/las")

        header ,pointData = readLASFile(lasFilePath+os.sep+lasFileName)
        print(time.time()-ticks)
        pointBuffer += list(pointData)
        print(time.time()-ticks)
        del header
        del pointData
        gc.collect()
        print(time.time()-ticks)







