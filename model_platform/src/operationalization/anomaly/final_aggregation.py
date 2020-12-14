import datetime

from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import udf, array
from pyspark.sql.types import DoubleType

from config import *


def write_to_hive(spark, sdf, data_source_list):
    try:
        hive_context = HiveContext(spark.sparkContext)
        hive_context.setConf("hive.exec.dynamic.partition", "true")
        hive_context.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        database = TENANT_NAME
        table = "anomaly_score"
        # sdf.show()
        columns = ["name", "type", "time_window", "timestamp"] + ["as_" + data_source for data_source in
                                                                  data_source_list] + ["score", "y", "m", "d"]

        sdf.select(columns).write.insertInto(database + "." + table)

    except Exception as e:
        print(str(e))
        pass


def aggregate_and_write_anomaly_score(spark, data_source_list):
    sdf_list = list()

    data_source = data_source_list[0]
    query = "select max(timestamp) as timestamp from demo." + data_source + "_profile_score"
    timestamp_sdf = spark.sql(query)
    timestamp = timestamp_sdf.rdd.first().timestamp
    date = datetime.datetime.fromtimestamp(timestamp / 1000.0)
    d = date.day
    m = date.month
    y = date.year
    query = "select name,type,time_window,pas as as_" + data_source + " from demo." + data_source + "_profile_score where timestamp in (select max(timestamp) from demo." + data_source + "_profile_score)"
    sdf = spark.sql(query)

    for i in range(1, len(data_source_list)):
        data_source = DATA_SOURCE_LIST[i]
        temp_query = "select name,type,time_window,pas as as_" + data_source + " from demo." + data_source + "_profile_score where timestamp in (select max(timestamp) from demo." + data_source + "_profile_score)"
        temp_sdf = spark.sql(temp_query)
        sdf_list.append(temp_sdf)

    for sdf_2 in sdf_list:
        sdf = sdf.join(sdf_2, on=["name", "type", "time_window"], how="full").fillna(0.0)

    sdf.show(20)
    sdf = sdf.withColumn("timestamp", lit(timestamp))
    sdf = sdf.withColumn("y", lit(y))
    sdf = sdf.withColumn("m", lit(m))
    sdf = sdf.withColumn("d", lit(d))

    max_cols = udf(lambda arr: max(arr), DoubleType())

    cols = ["as_" + data_source for data_source in data_source_list]

    sdf = sdf.withColumn('score', max_cols(array(cols))).persist()

    sdf.show(20)

    write_to_hive(spark=spark, sdf=sdf, data_source_list=data_source_list)


def main():
    app_name = TENANT_NAME + "_anomaly_final_score"
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    # spark = SparkSession.builder.appName(app_name).getOrCreate()
    data_source_list = DATA_SOURCE_LIST

    sc = spark.sparkContext
    sc.setLogLevel(PYSPARK_LOGLEVEL)

    aggregate_and_write_anomaly_score(spark=spark, data_source_list=data_source_list)


if __name__ == "__main__":
    main()
