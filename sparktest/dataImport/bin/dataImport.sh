#!/usr/bin/env bash
HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase classpath` hadoop jar dataImport.jar customdata2G hdfs://master:9000/pzx/custom