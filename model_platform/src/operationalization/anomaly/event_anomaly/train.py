import argparse

from config import *
from model_platform.src.model.anomaly.event_model.cluster_profile_model import \
    ClusterEventModel
from utils.data_store.kafka_data_store import KafkaDataStore
from utils.logging.notify_error import send_error_notification
from utils.logging.pylogger import get_log_path, configure_logger


def train_and_save_pyspark_kmeans_event_anomaly_model(spark, data_store, data_source, categorical_colnames,
                                                      numerical_colnames, logger, sdf):
    logger.info("###################################################################################")
    logger.info(
        "pyspark_kmeans event anomaly model training for {data_source} started".format(data_source=data_source))

    model = ClusterEventModel.train(spark=spark, data_store=data_store, cat_colnames=categorical_colnames,
                                    num_colnames=numerical_colnames,
                                    data_source=data_source, tenant_name=TENANT_NAME,
                                    anomaly_type=EVENT_ANOMALY_TYPE, sdf=sdf)

    model_path = EVENT_ANOMALY_MODEL_PATH.format(data_source=data_source,
                                                 anomaly_type=EVENT_ANOMALY_TYPE,
                                                 model_name=PYSPARK_KMEANS_MODEL)
    model.save(path=model_path)
    logger.info(
        "pyspark_kmeans event anomaly model training  for {data_source} ended".format(data_source=data_source))
    logger.info("###################################################################################")


def train_and_save_event_anomaly_model(spark, data_store, data_source, categorical_colnames, numerical_colnames,
                                       logger):
    logger.info("###################################################################################")
    logger.info("event anomaly model training for {data_source} started".format(data_source=data_source))

    broker_list = KAFKA_BROKER_LIST.split(",")
    sdf = data_store.read_spark_df_from_data_store(spark=spark,
                                                   tenant_name=TENANT_NAME,
                                                   data_source=data_source,
                                                   categorical_colnames=categorical_colnames,
                                                   numerical_colnames=numerical_colnames,
                                                   broker_list=broker_list,
                                                   kafka_topic=KAFKA_TOPIC,
                                                   kafka_group_id=KAFKA_GROUP_ID)

    sdf.show(3)

    train_and_save_pyspark_kmeans_event_anomaly_model(spark=spark, data_store=data_store, data_source=data_source,
                                                      categorical_colnames=categorical_colnames,
                                                      numerical_colnames=numerical_colnames,
                                                      logger=logger, sdf=sdf)

    logger.info("event anomaly model training for {data_source} ended".format(data_source=data_source))
    logger.info("###################################################################################")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--params", required=True)
    args = vars(ap.parse_args())
    params = json.loads(args["params"])
    data_source = params["data_source"]
    categorical_colnames = EVENT_MODEL_CATEGORICAL_COLUMNS
    numerical_colnames = EVENT_MODEL_NUMERICAL_COLUMNS

    log_path = get_log_path(data_source, "event_anomaly_train")
    logger = configure_logger(logger_name="event_anomaly_train", log_path=log_path, log_level=LOG_LEVEL)

    logger.info(data_source)
    logger.info(categorical_colnames)
    logger.info(numerical_colnames)
    from pyspark.sql import SparkSession

    app_name = data_source + "_event_anomaly_train"
    spark = SparkSession.builder.appName(app_name).enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel(PYSPARK_LOGLEVEL)

    try:
        data_store = KafkaDataStore(spark=spark)
        train_and_save_event_anomaly_model(spark=spark, data_store=data_store, data_source=data_source,
                                           categorical_colnames=categorical_colnames,
                                           numerical_colnames=numerical_colnames, logger=logger)
    except Exception as e:
        logger.exception(e)
        send_error_notification()


if __name__ == "__main__":
    main()
