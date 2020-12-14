import json
# path for tenant related data
TENANT_NAME = "demo"
BASE_PATH = "/user/elysium/" + TENANT_NAME

LOG_FOLDER = "/tmp"
LOG_LEVEL= "INFO"
PYSPARK_LOGLEVEL = "ERROR"

# repositary path
ANOMALY_MODEL_REPOSITORY = BASE_PATH + "/models_data/model"
ANOMALY_DATA_REPOSITORY = BASE_PATH + "/models_data/data"

# training data path
USER_PROFILE_DATA_PATH = ANOMALY_DATA_REPOSITORY + "/{data_source}/{entity_type}/{anomaly_type}/{time_window}.json"

# model paths
PROFILE_ANOMALY_MODEL_PATH = ANOMALY_MODEL_REPOSITORY + "/{data_source}/{anomaly_type}/{entity_type}/{time_window}/{model_name}"
EVENT_ANOMALY_MODEL_PATH = ANOMALY_MODEL_REPOSITORY + "/{data_source}/{anomaly_type}/{model_name}"

# time window

TIME_WINDOW_VALUE_UNIT_DICT = {"hour": {"value": 1, "unit": "HOURS"},
                               "day": {"value": 24, "unit": "HOURS"},
                               "week": {"value": 7*24, "unit": "HOURS"},
                               "month": {"value": 30*24, "unit": "HOURS"}}

TIME_WINDOW_TIMESTAMP_DICT = {"hour": TIME_WINDOW_VALUE_UNIT_DICT["hour"]["value"] * 60 * 60 * 1000,
                              "day": TIME_WINDOW_VALUE_UNIT_DICT["day"]["value"] * 60 * 60 * 1000,
                              "week": TIME_WINDOW_VALUE_UNIT_DICT["week"]["value"] * 60 * 60 * 1000,
                              "month": TIME_WINDOW_VALUE_UNIT_DICT["month"]["value"] * 60 * 60 * 1000}

TIME_WINDOW_LIST = TIME_WINDOW_TIMESTAMP_DICT.keys()

# entity types
USER_TYPE = "user"
ENTITY_TYPE = "entity"

# anomaly types
PROFILE_ANOMALY_TYPE = "profile"
EVENT_ANOMALY_TYPE = "event"

# model types
PYSPARK_KMEANS_MODEL = "pyspark_kmeans"
SKLEARN_ISOLATION_FOREST_MODEL = "sklearn_isolationforest"
SKLEARN_ONECLASS_SVM_MODEL = "sklearn_oneclasssvm"


SLACK_CHANNEL = "#att-models-log"
SLACK_BOT_TOKEN = "xoxb-501492022263-574304627685-xkt9wroHCGig8NQhZyJYinkt"

# HBASE co-ordinates
USER_UPDNRATIO_URL = "http://"+"10.10.150.21"+":"+"8990"+"/$attribute_name$per"+USER_TYPE+"?tenantid={tenant_name}&source={source}&value={value}&unit={unit}"
IP_UPDNRATIO_URL = "http://"+"10.10.150.21"+":"+"8990"+"/$attribute_name$per"+ENTITY_TYPE+"?tenantid={tenant_name}&source={source}&value={value}&unit={unit}"

DEPLOYMENT_TYPE = "dev"



ALERT_WEIGHT_JSON = """{"is_nu":14,"is_ne":5,"is_uatone":15,"is_uafromne":4,"is_nwpu":16,"is_nwpp":11,"is_nactpu":10,"is_nactpp":17,"is_nacnpu":19,"is_nacnpp":1}"""
ALERT_WEIGHT_DICT = json.loads(ALERT_WEIGHT_JSON)

KAFKA_BROKER_LIST = "10.10.150.21:6667,10.10.150.22:6667,10.10.150.23:6667"

KAFKA_TOPIC = "indexing"

SPARK_STREAMING_CHECKPOINT_PATH = "/tmp/checkpoint"

DATA_SOURCE_STRING_LIST = "wgtraffic,bluecoat"
DATA_SOURCE_LIST = DATA_SOURCE_STRING_LIST.split(",")

PROFILE_MODEL_CATEGORICAL_COLUMNS_STRING = "null"
PROFILE_MODEL_CATEGORICAL_COLUMNS = [] if PROFILE_MODEL_CATEGORICAL_COLUMNS_STRING == "null" else PROFILE_MODEL_CATEGORICAL_COLUMNS_STRING.split(",")
PROFILE_MODEL_NUMERICAL_COLUMNS_STRING = "updnratio"
PROFILE_MODEL_NUMERICAL_COLUMNS = [] if PROFILE_MODEL_NUMERICAL_COLUMNS_STRING == "null" else PROFILE_MODEL_NUMERICAL_COLUMNS_STRING.split(",")

EVENT_MODEL_CATEGORICAL_COLUMNS_STRING = "ip_src_addr,ip_dst_addr,ip_src_port,ip_dst_port"
EVENT_MODEL_CATEGORICAL_COLUMNS = [] if EVENT_MODEL_CATEGORICAL_COLUMNS_STRING == "null" else EVENT_MODEL_CATEGORICAL_COLUMNS_STRING.split(",")
EVENT_MODEL_NUMERICAL_COLUMNS_STRING = "in_bytes,out_bytes"
EVENT_MODEL_NUMERICAL_COLUMNS = [] if EVENT_MODEL_NUMERICAL_COLUMNS_STRING == "null" else EVENT_MODEL_NUMERICAL_COLUMNS_STRING.split(",")

EVENT_SCORING_STREAMING_WINDOW = 180

KAFKA_GROUP_ID = "r2kafka"

MAX_EVENT_RECORD_COUNT=1500
