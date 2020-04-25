#!/usr/bin/env bash
HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase classpath` hadoop jar dataImport.jar com.pzx.DataImport customdata2G hdfs://master:9000/pzx/custom

HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase classpath` hadoop jar dataImport.jar com.pzx.CreateHrc streetTxtData

HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase classpath` hadoop jar dataImport.jar com.pzx.CreateHrc hdfs://master:9000/pzx/custom

spark-submit --conf spark.driver.extraClassPath="/usr/local/spark-2.4.4/hbase-jars/*" \
--conf spark.executor.extraClassPath="/usr/local/spark-2.4.4/hbase-jars/*" \
 --class com.pzx.BulkLoad dataImport_bulkload.jar hdfs://master:9000/pzx/custom bulkLoadTestTable

#增加HBase classpath
--conf spark.driver.extraClassPath="/usr/local/spark-2.4.4/hbase-jars/*" \
--conf spark.executor.extraClassPath="/usr/local/spark-2.4.4/hbase-jars/*"
