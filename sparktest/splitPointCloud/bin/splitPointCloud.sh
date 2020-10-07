#!/usr/bin/env bash
spark-submit \
--driver-memory 10g \
--executor-memory 2g \
--class com.pzx.dataSplit.LasSplit \
--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:/home/pzx/log4j-driver.properties" \
splitPointCloud.jar laz hdfs://master:9000/pzx/custom


spark-submit --driver-memory 2g --executor-memory 5g --class com.pzx.dataSplit.TxtSplit1 \
--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:/home/pzx/log4j-driver.properties" \
split_txt.jar hdfs://master:9000/pzx/txtdata hdfs://master:9000/pzx/custom

spark-submit --driver-memory 2g --executor-memory 4g --executor-cores 1 --class com.pzx.dataSplit.TxtSplit2 \
--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:/home/pzx/log4j-driver.properties" \
--conf spark.memory.fraction=0.6 --conf spark.memory.storageFraction=0.4  \
--conf spark.driver.extraClassPath="/usr/local/spark-2.4.4/hbase-jars/*" \
--conf spark.executor.extraClassPath="/usr/local/spark-2.4.4/hbase-jars/*" \
--conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/tmp/splitPointCloudGC.log -XX:+PrintGCDateStamps" \
--conf spark.memory.offHeap.enabled="true"  \
--conf spark.memory.offHeap.size="1048576" \
--conf spark.executor.extraJavaOptions="-Xmn2g" \
splitPointCloud_new.jar hdfs://master:9000/pzx/txtdata/sg27.txt hdfs://master:9000/pzx/custom streetTestData

spark-submit --driver-memory 2g --executor-memory 4g --executor-cores 1 --class com.pzx.dataSplit.TxtSplit3 \
--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:/home/pzx/log4j-driver.properties" \
--conf spark.memory.storageFraction=0.4  \
--conf spark.memory.offHeap.enabled="true"  \
--conf spark.memory.offHeap.size="1048576" \
--conf spark.executor.extraJavaOptions="-Xmn2g" \
splitPointCloud.jar hdfs://master:9000/pzx/txtdata/test.txt hdfs://master:9000/pzx/custom



--conf spark.driver.extraJavaOptions="-Dlog4j.debug=true" \
--conf spark.driver.extraClassPath="/usr/local/spark-2.4.4/hbase-jars/*" \
--conf spark.executor.extraClassPath="/usr/local/spark-2.4.4/hbase-jars/*"
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=log4j-executor.properties" \
--files /home/pzx/log4j-driver.properties,/home/pzx/log4j-executor.properties   #上面的-Dlog4j.configuration不能直接读取到--files里面上传的文件 不知道为什么


spark-submit --driver-memory 2g --executor-memory 15g --class com.pzx.dataSplit.TxtSplit2 \
--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:/home/pzx/log4j-driver.properties" \
--conf spark.memory.fraction="0.3" --conf spark.executor.extraJavaOptions="-Xloggc:/tmp/gc.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"  \
splitPointCloud_new.jar hdfs://master:9000/pzx/txtdata/test.txt hdfs://master:9000/pzx/custom