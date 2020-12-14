from pyspark.sql import HiveContext, Row
from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import monotonically_increasing_id



from config import *
from model_platform.src.model.anomaly.event_model.cluster_profile_model import ClusterEventModel
from utils.logging.pylogger import get_log_path, configure_logger


def load_pyspark_kmeans_event_anomaly_model(spark, data_source):
    model_path = EVENT_ANOMALY_MODEL_PATH.format(data_source=data_source, anomaly_type=EVENT_ANOMALY_TYPE,
                                                 model_name=PYSPARK_KMEANS_MODEL)
    model = ClusterEventModel.load(spark=spark, path=model_path)
    return model


def load_event_anomaly_model(spark, data_source):
    model_dict = dict()
    model_dict[PYSPARK_KMEANS_MODEL] = load_pyspark_kmeans_event_anomaly_model(spark=spark, data_source=data_source)
    return model_dict


def get_event_anomaly_score(data_source, model_dict, input_df):
    result_df = model_dict[data_source][PYSPARK_KMEANS_MODEL].score(sdf=input_df)
    return result_df


def write_to_hive(time, rdd):
    def process_row(x):
        row_dict = dict()
        row_dict["timestamp"] = 0 if "timestamp" not in x else x["timestamp"]
        row_dict["source_type"] = "" if "source.type" not in x else x["source.type"]
        row_dict["user_name"] = "" if "src_user_name" not in x else x["src_user_name"]
        row_dict["entity_name"] = "" if "ip_src_addr" not in x else x["ip_src_addr"]
        row_dict["guid"] = "" if "guid" not in x else x["guid"]
        row_dict["alert_score"] = 0.0 if "alert_score" not in x else x["alert_score"]
        row_dict["alerts"] = "" if "alerts" not in x else x["alerts"]
        row_dict["y"] = 0 if "y" not in x else x["y"]
        row_dict["m"] = None if "m" not in x else x["m"]
        row_dict["d"] = None if "d" not in x else x["d"]
        for numerical_colname in EVENT_MODEL_NUMERICAL_COLUMNS:
            row_dict[numerical_colname] = 0.0 if numerical_colname not in x else float(x[numerical_colname])
        for categorical_colname in EVENT_MODEL_CATEGORICAL_COLUMNS:
            row_dict[categorical_colname] = "" if categorical_colname not in x else str(x[categorical_colname])

        row = Row(**row_dict)

        return row

    try:
        spark = SparkSession \
            .builder \
            .appName("event-anomaly-online-score") \
            .enableHiveSupport() \
            .getOrCreate()
        hive_context = HiveContext(spark.sparkContext)
        hive_context.setConf("hive.exec.dynamic.partition", "true")
        hive_context.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        row_rdd = rdd.map(process_row)
        sdf = hive_context.createDataFrame(row_rdd)
        sdf = sdf.drop_duplicates(subset=["guid"])
        sdf.cache()
        source_type_list = [TENANT_NAME + "_" + data_source for data_source in DATA_SOURCE_LIST]
        model_dict = dict()
        for data_source in DATA_SOURCE_LIST:
            model_dict[data_source] = load_event_anomaly_model(spark=spark, data_source=data_source)

        for source_type in source_type_list:
            sdf_source = sdf.filter(sdf.source_type == source_type)
            if not sdf_source.rdd.isEmpty():
                sdf_source.cache()
                database = source_type.split("_")[0]
                data_source = source_type.split("_")[1]
                table = data_source + "_event_alert_score"
                sdf_source.show(3)
                eas_sdf = get_event_anomaly_score(data_source=data_source, model_dict=model_dict,
                                                  input_df=sdf_source)
                result_sdf = sdf_source.join(eas_sdf.select(["guid", "EAS"]), on="guid", how="left")
                result_sdf = result_sdf.na.fill(0.0, subset=["EAS"])
                result_sdf.show(3)
                result_sdf.select("guid", "timestamp", "user_name", "entity_name", "source_type", "alerts",
                                  "alert_score",
                                  "EAS", "y", "m", "d").write.insertInto(database + "." + table)

    except Exception as e:
        pass


def create_context(logger):
    def binary_to_dict(x):
        my_dict = json.loads(x)
        return my_dict

    def get_alert_score(x):
        alert_weight_dict = ALERT_WEIGHT_DICT
        alert_score = 0.0
        alerts_string = ""
        if "alerts" in x and len(x["alerts"]) > 0:
            alerts_string = x["alerts"]
            for alert_string in alerts_string.split(","):
                alert_name = alert_string.split("=")[0]
                alert_state = alert_string.split("=")[1]
                if alert_state.lower() == "true":
                    alert_weight = 0.0
                    if alert_name in alert_weight_dict:
                        alert_weight = alert_weight_dict[alert_name]
                    alert_score += alert_weight

        x["alert_score"] = alert_score
        x["alerts"] = alerts_string
        return x

    spark = SparkSession \
        .builder \
        .appName("event-anomaly-online-score") \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(sc, EVENT_SCORING_STREAMING_WINDOW)

    broker_list, topic = KAFKA_BROKER_LIST, KAFKA_TOPIC

    kvs = KafkaUtils.createDirectStream(ssc, [topic],
                                        {"metadata.broker.list": broker_list, "auto.offset.reset": "largest"})

    source_type_list = [TENANT_NAME + "_" + data_source for data_source in DATA_SOURCE_LIST]
    lines = kvs.map(lambda x: x[1]).map(binary_to_dict).filter(lambda x: x["source.type"] in source_type_list).map(
        get_alert_score)
    lines.foreachRDD(write_to_hive)
    return ssc


def main():
    log_path = get_log_path("event", "anomaly_online_score")
    logger = configure_logger(logger_name="event_anomaly_online_score", log_path=log_path, log_level=LOG_LEVEL)

    logger.info(DATA_SOURCE_LIST)

    logger.info("Creating new context")
    ssc = StreamingContext.getOrCreate(SPARK_STREAMING_CHECKPOINT_PATH,
                                       lambda: create_context(logger=logger))
    logger.info("Started new context")
    ssc.sparkContext.setLogLevel(PYSPARK_LOGLEVEL)
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
