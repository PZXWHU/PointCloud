#!/usr/bin/env bash
spark-submit --driver-memory 8g --executor-memory 2g splitPointCloud.jar las hdfs://master:9000/pzx/custom