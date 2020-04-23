#!/usr/bin/env bash
HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase classpath` hadoop jar dataImport.jar com.pzx.DataImport customdata2G hdfs://master:9000/pzx/custom

HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase classpath` hadoop jar dataImport.jar com.pzx.CreateHrc streetTxtData

HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase classpath` hadoop jar dataImport.jar com.pzx.CreateHrc hdfs://master:9000/pzx/custom