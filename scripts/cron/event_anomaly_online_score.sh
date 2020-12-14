#!/usr/bin/env bash

export PYTHONPATH=/usr/hdp/current/spark2-client/python:/usr/hdp/current/spark2-client/python/lib/py4j-0.10.4-src.zip
export PYSPARK_PYTHON=/usr/local/bin/python3.6
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.6
export PYTHONPATH="/Users/tuhinsharma/Documents/sstech/projects/sia/models":$PYTHONPATH
/usr/hdp/current/spark2-client/bin/spark-submit --master local --deploy-mode client --driver-memory 4g --executor-memory 8g --executor-cores 2 --num-executors 2 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 /Users/tuhinsharma/Documents/sstech/projects/sia/models/model_platform/src/operationalization/anomaly/event_anomaly/online_score.py
