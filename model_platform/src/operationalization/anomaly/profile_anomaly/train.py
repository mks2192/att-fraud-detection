import argparse

from pyspark.sql import SparkSession

from config import *
from model_platform.src.model.anomaly.profile_model.cluster_profile_model import \
    ClusterProfileModel
from model_platform.src.model.anomaly.profile_model.isolation_forest_profile_model import \
    IsolationForestProfileModel
from model_platform.src.model.anomaly.profile_model.oneclasssvm_profile_model import \
    OneClassSVMProfileModel
from utils.data_store.hbase_data_store import HBaseDataStore
from utils.logging.notify_error import send_error_notification
from utils.logging.pylogger import get_log_path, configure_logger


def train_and_save_pyspark_kmeans_profile_anomaly_model_for_actor(spark, data_store, data_source, categorical_colnames,
                                                                  numerical_colnames, time_window, sdf, data_path,
                                                                  hbase_url, actor_type, logger):
    logger.info("###################################################################################")
    logger.info(
        "pyspark_kmeans profile anomaly model training for {actor_type} for {data_source} for time window {window} started".format(
            actor_type=actor_type,
            data_source=data_source,
            window=time_window))

    model = ClusterProfileModel.train(spark=spark, data_store=data_store,
                                      categorical_colnames=categorical_colnames,
                                      numerical_colnames=numerical_colnames, data_path=data_path,
                                      data_source=data_source,
                                      actor_type=actor_type,
                                      sdf=sdf,
                                      anomaly_type=PROFILE_ANOMALY_TYPE,
                                      time_window=time_window, hbase_url=hbase_url)
    model_path = PROFILE_ANOMALY_MODEL_PATH.format(data_source=data_source,
                                                   entity_type=actor_type, anomaly_type=PROFILE_ANOMALY_TYPE,
                                                   time_window=time_window,
                                                   model_name=PYSPARK_KMEANS_MODEL)
    model.save(path=model_path)
    logger.info(
        "pyspark_kmeans profile anomaly model training for {actor_type} for {data_source} for time window {window} ended".format(
            actor_type=actor_type,
            data_source=data_source,
            window=time_window))
    logger.info("###################################################################################")


def train_and_save_sklearn_isolationforest_profile_anomaly_model_for_actor(spark, data_store, data_source,
                                                                           categorical_colnames,
                                                                           numerical_colnames, time_window, sdf,
                                                                           data_path,
                                                                           hbase_url, actor_type, logger):
    logger.info("###################################################################################")
    logger.info(
        "sklearn_isolationforest profile anomaly model training for {actor_type} for {data_source} for time window {window} started".format(
            actor_type=actor_type,
            data_source=data_source, window=time_window))

    model = IsolationForestProfileModel.train(data_store=data_store, categorical_colnames=categorical_colnames,
                                              numerical_colnames=numerical_colnames,
                                              data_path=data_path, data_source=data_source,
                                              actor_type=actor_type,
                                              anomaly_type=PROFILE_ANOMALY_TYPE,
                                              sdf=sdf,
                                              time_window=time_window, hbase_url=hbase_url)

    model_path = PROFILE_ANOMALY_MODEL_PATH.format(data_source=data_source,
                                                   entity_type=actor_type, anomaly_type=PROFILE_ANOMALY_TYPE,
                                                   time_window=time_window,
                                                   model_name=SKLEARN_ISOLATION_FOREST_MODEL)
    model.save(spark=spark, path=model_path)
    logger.info(
        "sklearn_isolationforest profile anomaly model training for {actor_type} for {data_source} for time window {window} ended".format(
            actor_type=actor_type,
            data_source=data_source, window=time_window))
    logger.info("###################################################################################")


def train_and_save_sklearn_oneclasssvm_profile_anomaly_model_for_actor(spark, data_store, data_source,
                                                                       categorical_colnames,
                                                                       numerical_colnames, time_window, sdf, data_path,
                                                                       hbase_url, actor_type, logger):
    logger.info("###################################################################################")
    logger.info(
        "sklearn_oneclasssvm profile anomaly model training for {actor_type} for {data_source} for time window {window} started".format(
            actor_type=actor_type,
            data_source=data_source,
            window=time_window))

    model = OneClassSVMProfileModel.train(data_store=data_store, categorical_colnames=categorical_colnames,
                                          numerical_colnames=numerical_colnames,
                                          data_path=data_path, data_source=data_source,
                                          actor_type=actor_type,
                                          anomaly_type=PROFILE_ANOMALY_TYPE,
                                          sdf=sdf,
                                          time_window=time_window, hbase_url=hbase_url)

    model_path = PROFILE_ANOMALY_MODEL_PATH.format(data_source=data_source,
                                                   entity_type=actor_type, anomaly_type=PROFILE_ANOMALY_TYPE,
                                                   time_window=time_window,
                                                   model_name=SKLEARN_ONECLASS_SVM_MODEL)
    model.save(spark=spark, path=model_path)
    logger.info(
        "sklearn_oneclasssvm profile anomaly model training for {actor_type} for {data_source} for time window {window} ended".format(
            actor_type=actor_type,
            data_source=data_source,
            window=time_window))
    logger.info("###################################################################################")


def train_and_save_profile_anomaly_model_for_actor(spark, data_store, data_source, categorical_colnames,
                                                   numerical_colnames, actor_type, logger):
    logger.info("###################################################################################")
    logger.info(
        "profile anomaly model training for {actor_type} for {data_source} started".format(actor_type=actor_type,
                                                                                           data_source=data_source))
    for time_window in TIME_WINDOW_LIST:
        data_path = USER_PROFILE_DATA_PATH.format(data_source=data_source, entity_type=actor_type,
                                                  anomaly_type=PROFILE_ANOMALY_TYPE,
                                                  time_window=time_window)
        if actor_type == ENTITY_TYPE:
            url = IP_UPDNRATIO_URL
        else:
            url = USER_UPDNRATIO_URL
        hbase_url = url.format(tenant_name=TENANT_NAME, source=data_source,
                               value=TIME_WINDOW_VALUE_UNIT_DICT[time_window]["value"],
                               unit=TIME_WINDOW_VALUE_UNIT_DICT[time_window]["unit"])

        sdf = data_store.read_spark_df_from_data_store(spark=spark,
                                                       data_source=data_source,
                                                       categorical_colnames=categorical_colnames,
                                                       numerical_colnames=numerical_colnames,
                                                       data_path=data_path, hbase_url=hbase_url,
                                                       time_window=time_window,
                                                       actor_type=actor_type,
                                                       logger=logger)

        sdf.show(3)

        train_and_save_pyspark_kmeans_profile_anomaly_model_for_actor(spark=spark, data_store=data_store,
                                                                      data_source=data_source,
                                                                      categorical_colnames=categorical_colnames,
                                                                      numerical_colnames=numerical_colnames,
                                                                      time_window=time_window, sdf=sdf,
                                                                      data_path=data_path,
                                                                      hbase_url=hbase_url,
                                                                      actor_type=actor_type,
                                                                      logger=logger)
        train_and_save_sklearn_isolationforest_profile_anomaly_model_for_actor(spark=spark, data_store=data_store,
                                                                               data_source=data_source,
                                                                               categorical_colnames=categorical_colnames,
                                                                               numerical_colnames=numerical_colnames,
                                                                               data_path=data_path,
                                                                               hbase_url=hbase_url,
                                                                               time_window=time_window, sdf=sdf,
                                                                               actor_type=actor_type,
                                                                               logger=logger)
        train_and_save_sklearn_oneclasssvm_profile_anomaly_model_for_actor(spark=spark, data_store=data_store,
                                                                           data_source=data_source,
                                                                           categorical_colnames=categorical_colnames,
                                                                           numerical_colnames=numerical_colnames,
                                                                           time_window=time_window, sdf=sdf,
                                                                           data_path=data_path,
                                                                           hbase_url=hbase_url,
                                                                           actor_type=actor_type,
                                                                           logger=logger)
    logger.info("profile anomaly model training for {actor_type} for {data_source} ended".format(actor_type=actor_type,
                                                                                                 data_source=data_source))
    logger.info("###################################################################################")


def train_and_save_profile_anomaly_model(spark, data_store, data_source, categorical_colnames, numerical_colnames,
                                         logger):
    logger.info("###################################################################################")
    logger.info("profile anomaly model training for {data_source} started".format(data_source=data_source))
    train_and_save_profile_anomaly_model_for_actor(spark=spark, data_store=data_store, data_source=data_source,
                                                   categorical_colnames=categorical_colnames,
                                                   numerical_colnames=numerical_colnames, actor_type=ENTITY_TYPE,
                                                   logger=logger)
    train_and_save_profile_anomaly_model_for_actor(spark=spark, data_store=data_store, data_source=data_source,
                                                   categorical_colnames=categorical_colnames,
                                                   numerical_colnames=numerical_colnames, actor_type=USER_TYPE,
                                                   logger=logger)
    logger.info("profile anomaly model training for {data_source} ended".format(data_source=data_source))
    logger.info("###################################################################################")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--params", required=True)
    args = vars(ap.parse_args())
    params = json.loads(args["params"])
    data_source = params["data_source"]
    categorical_colnames = PROFILE_MODEL_CATEGORICAL_COLUMNS
    numerical_colnames = PROFILE_MODEL_NUMERICAL_COLUMNS

    log_path = get_log_path(data_source, "profile_anomaly_train")
    logger = configure_logger(logger_name="profile_anomaly_train", log_path=log_path, log_level=LOG_LEVEL)

    logger.info(data_source)
    logger.info(categorical_colnames)
    logger.info(numerical_colnames)

    app_name = data_source + "_profile_anomaly_train"
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel(PYSPARK_LOGLEVEL)

    try:
        # data_store = HDFSDataStore(spark=spark)
        data_store = HBaseDataStore(spark=spark)
        # data_store = DatagenDataStore(spark=spark,type="train")
        train_and_save_profile_anomaly_model(spark=spark, data_store=data_store, data_source=data_source,
                                             categorical_colnames=categorical_colnames,
                                             numerical_colnames=numerical_colnames, logger=logger)
    except Exception as e:
        logger.exception(e)
        send_error_notification()


if __name__ == "__main__":
    main()
