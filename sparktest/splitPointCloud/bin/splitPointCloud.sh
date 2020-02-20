#!/usr/bin/env bash
spark-submit \
--driver-memory 10g \
--executor-memory 2g \
--class com.pzx.LasSplit \
--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:/home/pzx/log4j-driver.properties" \
splitPointCloud.jar laz hdfs://master:9000/pzx/custom



--conf spark.driver.extraJavaOptions="-Dlog4j.debug=true" \
--conf spark.driver.extraClassPath="/usr/local/spark-2.4.4/hbase-jars/*" \
--conf spark.executor.extraClassPath="/usr/local/spark-2.4.4/hbase-jars/*"
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=log4j-executor.properties" \
--files /home/pzx/log4j-driver.properties,/home/pzx/log4j-executor.properties   #上面的-Dlog4j.configuration不能直接读取到--files里面上传的文件 不知道为什么