#!/usr/bin/env bash
spark-submit --driver-memory 4g splitPointCloud.jar /tmp/las hdfs://master:9000/pzx/custom