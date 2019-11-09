# -*- encoding: utf-8 -*-
from hdfs import *
import os


def writerDataToFile(outputFilePath,myData = None,mode = "ab"):

    if(outputFilePath.startswith("http")):
        # outputFilePath = http://master:50070,/pzx/pysparkresult
        webHDFSAddress = outputFilePath.split(",")[0]
        outputHDFSFilePath = outputFilePath.split(",")[1]
        client = Client(webHDFSAddress)
        myEncoding = None
        myAppend = False
        if("b" not in mode):
            myEncoding = "utf-8"
        if("a" in mode):
            if(client.status(outputHDFSFilePath,strict=False)!=None):
                myAppend = True

        with client.write(outputHDFSFilePath,encoding=myEncoding,append=myAppend) as writer:
            writer.write(myData)

        return writer
    else:
        with open(outputFilePath, mode=mode) as writer:
            writer.write(myData)
        return writer


def listDir(outputFileDirPath):
    if(outputFileDirPath.startswith("http")):
        webHDFSAddress = outputFileDirPath.split(",")[0]
        outputHDFSFileDirPath = outputFileDirPath.split(",")[1]
        client = Client(webHDFSAddress)
        return client.list(outputHDFSFileDirPath)
    else:
        return os.listdir(outputFileDirPath)