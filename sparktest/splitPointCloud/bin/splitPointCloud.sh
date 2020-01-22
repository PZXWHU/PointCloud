#!/usr/bin/env bash
spark-submit --driver-memory 10g --executor-memory 1g splitPointCloud.jar las hdfs://master:9000/pzx/custom