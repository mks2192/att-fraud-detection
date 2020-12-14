import argparse
import datetime
import time

from pyspark.sql import HiveContext, Row
from pyspark.sql import SparkSession

from config import *
from model_platform.src.model.anomaly.profile_model.cluster_profile_model import \
    ClusterProfileModel
from model_platform.src.model.anomaly.profile_model.isolation_forest_profile_model import \
    IsolationForestProfileModel
from model_platform.src.model.anomaly.profile_model.oneclasssvm_profile_model import \
    OneClassSVMProfileModel
from utils.data_store.hbase_data_store import HBaseDataStore
from utils.logging.pylogger import get_log_path, configure_logger


def load_pyspark_kmeans_profile_anomaly_model_for_ip(spark, data_source):
    model_dict = dict()
    for time_window in TIME_WINDOW_LIST:
        model_path = PROFILE_ANOMALY_MODEL_PATH.format(data_source=data_source,
                                                       entity_type=ENTITY_TYPE, anomaly_type=PROFILE_ANOMALY_TYPE,
                                                       time_window=time_window,
                                                       model_name=PYSPARK_KMEANS_MODEL)
        model = ClusterProfileModel.load(spark=spark, path=model_path)
        model_dict[time_window] = model
    return model_dict


def load_sklearn_isolationforest_profile_anomaly_model_for_ip(spark, data_source):
    model_dict = dict()
    for time_window in TIME_WINDOW_LIST:
        model_path = PROFILE_ANOMALY_MODEL_PATH.format(data_source=data_source,
                                                       entity_type=ENTITY_TYPE, anomaly_type=PROFILE_ANOMALY_TYPE,
                                                       time_window=time_window,
                                                       model_name=SKLEARN_ISOLATION_FOREST_MODEL)
        model = IsolationForestProfileModel.load(spark=spark, path=model_path)
        model_dict[time_window] = model
    return model_dict


def load_sklearn_oneclasssvm_profile_anomaly_model_for_ip(spark, data_source):
    model_dict = dict()
    for time_window in TIME_WINDOW_LIST:
        model_path = PROFILE_ANOMALY_MODEL_PATH.format(data_source=data_source,
                                                       entity_type=ENTITY_TYPE, anomaly_type=PROFILE_ANOMALY_TYPE,
                                                       time_window=time_window,
                                                       model_name=SKLEARN_ONECLASS_SVM_MODEL)
        model = OneClassSVMProfileModel.load(spark=spark, path=model_path)
        model_dict[time_window] = model
    return model_dict


def load_pyspark_kmeans_profile_anomaly_model_for_user(spark, data_source):
    model_dict = dict()
    for time_window in TIME_WINDOW_LIST:
        model_path = PROFILE_ANOMALY_MODEL_PATH.format(data_source=data_source,
                                                       entity_type=USER_TYPE, anomaly_type=PROFILE_ANOMALY_TYPE,
                                                       time_window=time_window,
                                                       model_name=PYSPARK_KMEANS_MODEL)
        model = ClusterProfileModel.load(spark=spark, path=model_path)
        model_dict[time_window] = model
    return model_dict


def load_sklearn_isolationforest_profile_anomaly_model_for_user(spark, data_source):
    model_dict = dict()
    for time_window in TIME_WINDOW_LIST:
        model_path = PROFILE_ANOMALY_MODEL_PATH.format(data_source=data_source,
                                                       entity_type=USER_TYPE, anomaly_type=PROFILE_ANOMALY_TYPE,
                                                       time_window=time_window,
                                                       model_name=SKLEARN_ISOLATION_FOREST_MODEL)
        model = IsolationForestProfileModel.load(spark=spark, path=model_path)
        model_dict[time_window] = model
    return model_dict


def load_sklearn_oneclasssvm_profile_anomaly_model_for_user(spark, data_source):
    model_dict = dict()
    for time_window in TIME_WINDOW_LIST:
        model_path = PROFILE_ANOMALY_MODEL_PATH.format(data_source=data_source,
                                                       entity_type=USER_TYPE, anomaly_type=PROFILE_ANOMALY_TYPE,
                                                       time_window=time_window,
                                                       model_name=SKLEARN_ONECLASS_SVM_MODEL)
        model = OneClassSVMProfileModel.load(spark=spark, path=model_path)
        model_dict[time_window] = model
    return model_dict


def load_profile_anomaly_model_for_ip(spark, data_source):
    model_dict = dict()
    model_dict[PYSPARK_KMEANS_MODEL] = load_pyspark_kmeans_profile_anomaly_model_for_ip(spark=spark,
                                                                                        data_source=data_source)
    model_dict[SKLEARN_ISOLATION_FOREST_MODEL] = load_sklearn_isolationforest_profile_anomaly_model_for_ip(spark=spark,
                                                                                                           data_source=data_source)
    model_dict[SKLEARN_ONECLASS_SVM_MODEL] = load_sklearn_oneclasssvm_profile_anomaly_model_for_ip(spark=spark,
                                                                                                   data_source=data_source)
    return model_dict


def load_profile_anomaly_model_for_user(spark, data_source):
    model_dict = dict()
    model_dict[PYSPARK_KMEANS_MODEL] = load_pyspark_kmeans_profile_anomaly_model_for_user(spark=spark,
                                                                                          data_source=data_source)
    model_dict[SKLEARN_ISOLATION_FOREST_MODEL] = load_sklearn_isolationforest_profile_anomaly_model_for_user(
        spark=spark, data_source=data_source)
    model_dict[SKLEARN_ONECLASS_SVM_MODEL] = load_sklearn_oneclasssvm_profile_anomaly_model_for_user(spark=spark,
                                                                                                     data_source=data_source)
    return model_dict


def load_profile_anomaly_model(spark, data_source):
    model_dict = dict()
    model_dict[ENTITY_TYPE] = load_profile_anomaly_model_for_ip(spark=spark, data_source=data_source)
    model_dict[USER_TYPE] = load_profile_anomaly_model_for_user(spark=spark, data_source=data_source)
    return model_dict


def get_profile_anomaly_score(spark, data_source, model_dict, input_sdf, time_window, entity_type):
    if input_sdf.rdd.isEmpty():
        return None
    input_df = input_sdf.toPandas()
    result_df_1 = model_dict[entity_type][PYSPARK_KMEANS_MODEL][time_window].score(
        input_sdf).toPandas()
    input_df["pas_kmeans"] = result_df_1["PAS"]
    result_df_2 = model_dict[entity_type][SKLEARN_ISOLATION_FOREST_MODEL][
        time_window].score(input_df)
    input_df["pas_isolation"] = result_df_2["PAS"]
    result_df_3 = model_dict[entity_type][SKLEARN_ONECLASS_SVM_MODEL][
        time_window].score(input_df)
    input_df["pas_svm"] = result_df_3["PAS"]
    del input_df['PAS']
    input_df["pas"] = input_df[["pas_kmeans", "pas_isolation", "pas_svm"]].mean(axis=1)

    return input_df


def write_to_hive(spark, rdd, key, time_window, timestamp, data_source):
    def process_line(x):
        row_dict = dict()
        row_dict["name"] = x[key]
        row_dict["type"] = key
        row_dict["time_window"] = time_window
        row_dict["timestamp"] = timestamp
        row_dict["pas_kmeans"] = x["pas_kmeans"]
        row_dict["pas_isolation"] = x["pas_isolation"]
        row_dict["pas_svm"] = x["pas_svm"]
        row_dict["pas"] = x["pas"]
        row_dict["d"] = d
        row_dict["m"] = m
        row_dict["y"] = y
        row = Row(**row_dict)
        return row

    try:
        hive_context = HiveContext(spark.sparkContext)
        hive_context.setConf("hive.exec.dynamic.partition", "true")
        hive_context.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        database = TENANT_NAME
        table = data_source + "_profile_score"
        date = datetime.datetime.fromtimestamp(timestamp / 1000.0)
        d = date.day
        m = date.month
        y = date.year
        row_rdd = rdd.map(lambda x: process_line(x))
        sdf = spark.createDataFrame(row_rdd)
        sdf = sdf.na.fill(0.0, subset=["pas_kmeans", "pas_isolation", "pas_svm", "pas"])
        sdf.select("name", "type", "time_window", "timestamp", "pas_kmeans", "pas_isolation", "pas_svm",
                   "pas", "y", "m", "d").write.insertInto(
            database + "." + table)

    except Exception as e:
        print(str(e))
        pass


def score_profile_anomaly_model_for_actor(spark, data_store, data_source, categorical_colnames,
                                          numerical_colnames, model_dict, timestamp, logger, actor_type):
    logger.info("###################################################################################")
    logger.info(
        "profile anomaly model scoring for {actor_type} for {data_source} started".format(actor_type=actor_type,
                                                                                          data_source=data_source))
    for time_window in TIME_WINDOW_LIST:
        if actor_type == ENTITY_TYPE:
            url = IP_UPDNRATIO_URL
        else:
            url = USER_UPDNRATIO_URL
        hbase_url = url.format(tenant_name=TENANT_NAME,
                               source=data_source,
                               value=TIME_WINDOW_VALUE_UNIT_DICT[time_window]["value"],
                               unit=TIME_WINDOW_VALUE_UNIT_DICT[time_window]["unit"])

        input_sdf = data_store.read_spark_df_from_data_store(spark=spark, numerical_colnames=numerical_colnames,
                                                             categorical_colnames=categorical_colnames,
                                                             time_window=time_window,
                                                             actor_type=actor_type, hbase_url=hbase_url, logger=logger)

        input_sdf.show(3)

        df = get_profile_anomaly_score(spark=spark, data_source=data_source, model_dict=model_dict, input_sdf=input_sdf,
                                       time_window=time_window,
                                       entity_type=actor_type)
        logger.info(time_window)

        if df is not None:
            rdd = spark.sparkContext.parallelize(df.to_dict(orient="records"))
            write_to_hive(spark=spark, rdd=rdd, key=actor_type, time_window=time_window, timestamp=timestamp,
                          data_source=data_source)
    logger.info("profile anomaly model scoring for {actor_type} for {data_source} ended".format(actor_type=actor_type,
                                                                                                data_source=data_source))
    logger.info("###################################################################################")


def score_profile_anomaly_model(spark, data_store, data_source, categorical_colnames, numerical_colnames, model_dict,
                                logger):
    logger.info("###################################################################################")
    logger.info("profile anomaly model scoring for {data_source} started".format(data_source=data_source))
    timestamp = int(time.time() * 1000)
    score_profile_anomaly_model_for_actor(spark=spark, data_store=data_store, data_source=data_source,
                                          categorical_colnames=categorical_colnames,
                                          numerical_colnames=numerical_colnames, model_dict=model_dict,
                                          timestamp=timestamp, logger=logger, actor_type=ENTITY_TYPE)
    score_profile_anomaly_model_for_actor(spark=spark, data_store=data_store, data_source=data_source,
                                          categorical_colnames=categorical_colnames,
                                          numerical_colnames=numerical_colnames, model_dict=model_dict,
                                          timestamp=timestamp, logger=logger, actor_type=USER_TYPE)
    logger.info("profile anomaly model scoring for {data_source} ended".format(data_source=data_source))
    logger.info("###################################################################################")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--params", required=True)
    args = vars(ap.parse_args())
    params = json.loads(args["params"])
    data_source = params["data_source"]
    categorical_colnames = PROFILE_MODEL_CATEGORICAL_COLUMNS
    numerical_colnames = PROFILE_MODEL_NUMERICAL_COLUMNS

    log_path = get_log_path(data_source, "profile_anomaly_batch_score")
    logger = configure_logger(logger_name="profile_anomaly_batch_score", log_path=log_path, log_level=LOG_LEVEL)

    logger.info(data_source)
    logger.info(categorical_colnames)
    logger.info(numerical_colnames)

    app_name = data_source + "_profile_anomaly_batch_score"
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    # spark = SparkSession.builder.appName(app_name).getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel(PYSPARK_LOGLEVEL)

    model_dict = load_profile_anomaly_model(spark=spark, data_source=data_source)
    logger.info(model_dict)

    data_store = HBaseDataStore(spark=spark)

    score_profile_anomaly_model(spark=spark, data_store=data_store, data_source=data_source,
                                categorical_colnames=categorical_colnames,
                                numerical_colnames=numerical_colnames, model_dict=model_dict, logger=logger)


if __name__ == "__main__":
    main()
