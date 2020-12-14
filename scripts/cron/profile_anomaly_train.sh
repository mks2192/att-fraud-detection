#!/usr/bin/env bash

export PYTHONPATH=/usr/hdp/current/spark2-client/python:/usr/hdp/current/spark2-client/python/lib/py4j-0.10.4-src.zip
export PYSPARK_PYTHON=/usr/local/bin/python3.6
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.6
export PYTHONPATH="/Users/tuhinsharma/Documents/sstech/projects/sia/models":$PYTHONPATH
/usr/hdp/current/spark2-client/bin/spark-submit /Users/tuhinsharma/Documents/sstech/projects/sia/models/model_platform/src/operationalization/anomaly/profile_anomaly/train.py --params '{"data_source":"wgtraffic"}'
/usr/hdp/current/spark2-client/bin/spark-submit /Users/tuhinsharma/Documents/sstech/projects/sia/models/model_platform/src/operationalization/anomaly/profile_anomaly/train.py --params '{"data_source":"bluecoat"}'
