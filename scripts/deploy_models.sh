#!/usr/bin/env bash

filename="config.py"
cp ./scripts/templates/config.py.template ./$filename


if [ -z "$1" ]
  then
    echo "Provide tenant id as First Argument"
fi

if [ -z "$2" ]
  then
    echo "Provide absolute directory path for logging as Second Argument"
fi

if [ -z "$3" ]
  then
    echo "Provide log level for project logging as Third Argument"
fi

if [ -z "$4" ]
  then
    echo "Provide log level for pyspark logging as Fourth Argument"
fi


if [ -z "${5}" ]
  then
    echo "Provide list of categorical columns for Event Data as Fifth Argument"
	exit 0
fi

if [ -z "${6}" ]
  then
    echo "Provide list of numerical columns for Event Data as Sixth Argument"
	exit 0
fi

if [ -z "$7" ]
  then
    echo "Provide slack channel as Seventh Argument"
fi

if [ -z "$8" ]
  then
    echo "Provide slack bot token as Eighth Argument"
fi

if [ -z "$9" ]
  then
    echo "Provide profile data server ip as Ninth Argument"
fi

if [ -z "${10}" ]
  then
    echo "Provide profile data server port as Tenth Argument"
fi

if [ -z "${11}" ]
  then
    echo "Provide deployment type as Eleventh Argument"
fi

if [ -z "${12}" ]
  then
    echo "Provide alert weight dict (use double quote only) as Twelveth Argument"
fi

if [ -z "${13}" ]
  then
    echo "Provide kafka broker list as Thirteenth Argument"
fi

if [ -z "${14}" ]
  then
    echo "Provide kafka topic name for indexing as Fourteenth Argument"
fi

if [ -z "${15}" ]
  then
    echo "Provide spark streaming checkpoint path as Fifteenth Argument"
fi

if [ -z "${16}" ]
  then
    echo "Provide list of data sources (comma separated) as Sixteenth Argument"
	exit 0
fi

if [ -z "${17}" ]
  then
    echo "Provide list of categorical columns for Profile Data as Seventeenth Argument"
	exit 0
fi

if [ -z "${18}" ]
  then
    echo "Provide list of numerical columns for Profile Data as Eighteenth Argument"
	exit 0
fi

if [ -z "${19}" ]
  then
    echo "Provide kafka group id for indexing as Nineteenth Argument"
	exit 0
fi


tenant_id=$1
log_folder=$(echo $2 | sed 's/\//\\\//g')
log_level=$(echo $3 | tr a-z A-Z)
pyspark_log_level=$(echo $4 | tr a-z A-Z)
event_categorical_column_string=$5
event_numerical_column_string=$6
slack_channel=$7
slack_bot_token=$8
profile_data_server_ip=$9
profile_data_server_port=${10}
deployment_type=${11}
alert_weight_dict=${12}
kafka_broker_list=${13}
kafka_topic=${14}
spark_streaming_checkpoint_path=$(echo ${15} | sed 's/\//\\\//g')
data_source_list=$(echo ${16} | tr "," "\n")
profile_categorical_column_string=${17}
profile_numerical_column_string=${18}
data_source_string_list=${16}
kafka_group_id=${19}


echo "Model Deployment for "$tenant_id" - started"
sed -ie "s/<TENANT-NAME>/\"$tenant_id\"/g" $filename
sed -ie "s/<LOG-FOLDER>/\"$log_folder\"/g" $filename
sed -ie "s/<LOG-LEVEL>/\"$log_level\"/g" $filename
sed -ie "s/<PYSPARK-LOGLEVEL>/\"$pyspark_log_level\"/g" $filename
sed -ie "s/<EVENT-MODEL-CATEGORICAL-COLUMNS-STRING>/\"$event_categorical_column_string\"/g" $filename
sed -ie "s/<EVENT-MODEL-NUMERICAL-COLUMNS-STRING>/\"$event_numerical_column_string\"/g" $filename
sed -ie "s/<SLACK-CHANNEL>/\"$slack_channel\"/g" $filename
sed -ie "s/<SLACK-BOT-TOKEN>/\"$slack_bot_token\"/g" $filename
sed -ie "s/<PROFILER-DATA-SERVER-IP>/\"$profile_data_server_ip\"/g" $filename
sed -ie "s/<PROFILER-DATA-SERVER-PORT>/\"$profile_data_server_port\"/g" $filename
sed -ie "s/<DEPLOYMENT-TYPE>/\"$deployment_type\"/g" $filename
sed -ie "s/<ALERT-WEIGHT-JSON>/\"$alert_weight_dict\"/g" $filename
sed -ie "s/<KAFKA-BROKER-LIST>/\"$kafka_broker_list\"/g" $filename
sed -ie "s/<KAFKA-TOPIC>/\"$kafka_topic\"/g" $filename
sed -ie "s/<SPARK-STREAMING-CHECKPOINT-PATH>/\"$spark_streaming_checkpoint_path\"/g" $filename
sed -ie "s/<DATA-SOURCE-LIST>/\"$data_source_string_list\"/g" $filename
sed -ie "s/<PROFILE-MODEL-CATEGORICAL-COLUMNS-STRING>/\"$profile_categorical_column_string\"/g" $filename
sed -ie "s/<PROFILE-MODEL-NUMERICAL-COLUMNS-STRING>/\"$profile_numerical_column_string\"/g" $filename
sed -ie "s/<KAFKA-GROUP-ID>/\"$kafka_group_id\"/g" $filename

hql_filename="./scripts/hive/hive.hql"
hql_template_filename="./scripts/templates/hive.hql.template"

echo "set hive.support.sql11.reserved.keywords=false;" > $hql_filename
echo "" >> $hql_filename
echo "-- create the database for "$tenant_id" if it does not exist" >> $hql_filename
echo "" >> $hql_filename
echo "CREATE DATABASE IF NOT EXISTS \`"$tenant_id"\`;" >> $hql_filename
for i in $data_source_list
do

    while IFS= read -r line;do
        line=${line/<TENANT-ID>/$tenant_id}
        line=${line/<TENANT-ID>/$tenant_id}
        line=${line/<DATA-SOURCE>/$i}
        line=${line/<DATA-SOURCE>/$i}
        echo "$line" >> $hql_filename
    done < $hql_template_filename
done

echo "" >> $hql_filename
echo "-- create table in the "$tenant_id" database for storing the aggregated anomaly score of entities and users across the data sources" >> $hql_filename
echo "" >> $hql_filename
var=""
for i in $data_source_list
do
    var=$var"as_"$i" double,"
done
echo "CREATE TABLE IF NOT EXISTS $tenant_id.anomaly_score ( \`name\` string, \`type\` string, \`time_window\` string, \`timestamp\` bigint,"$var"score double) PARTITIONED BY ( \`y\` int, \`m\` int, \`d\` int) STORED AS ORC LOCATION '/user/elysium/$tenant_id/models/anomaly_score';" >> $hql_filename


profile_anomaly_train_filename="./scripts/cron/profile_anomaly_train.sh"

cp ./scripts/templates/pyspark_env.sh.template $profile_anomaly_train_filename

for i in $data_source_list
do
    while IFS= read -r line;do
        line=${line/<DATA-SOURCE>/$i}
        echo "$line" >> $profile_anomaly_train_filename
    done < "./scripts/templates/profile_anomaly_train.sh.template"
done

pwd=$(echo `pwd` | sed 's/\//\\\//g')
sed -ie "s/<PWD>/$pwd/g" $profile_anomaly_train_filename
rm -rf $profile_anomaly_train_filename"e"

event_anomaly_train_filename="./scripts/cron/event_anomaly_train.sh"

cp ./scripts/templates/pyspark_env.sh.template $event_anomaly_train_filename

for i in $data_source_list
do
    while IFS= read -r line;do
        line=${line/<DATA-SOURCE>/$i}
        echo "$line" >> $event_anomaly_train_filename
    done < "./scripts/templates/event_anomaly_train.sh.template"
done

sed -ie "s/<PWD>/$pwd/g" $event_anomaly_train_filename
rm -rf $event_anomaly_train_filename"e"

event_anomaly_online_score_filename="./scripts/cron/event_anomaly_online_score.sh"

cp ./scripts/templates/pyspark_env.sh.template $event_anomaly_online_score_filename


while IFS= read -r line;do
    line=${line/<DATA-SOURCE-LIST>/${16}}
    echo "$line" >> $event_anomaly_online_score_filename
done < "./scripts/templates/event_anomaly_online_score.sh.template"

sed -ie "s/<PWD>/$pwd/g" $event_anomaly_online_score_filename
rm -rf $event_anomaly_online_score_filename"e"


profile_anomaly_batch_score_filename="./scripts/cron/profile_anomaly_batch_score.sh"

cp ./scripts/templates/pyspark_env.sh.template $profile_anomaly_batch_score_filename


for i in $data_source_list
do
    while IFS= read -r line;do
        line=${line/<DATA-SOURCE>/$i}
        echo "$line" >> $profile_anomaly_batch_score_filename
    done < "./scripts/templates/profile_anomaly_batch_score.sh.template"
done

echo "/usr/hdp/current/spark2-client/bin/spark-submit <PWD>/model_platform/src/operationalization/anomaly/final_aggregation.py" >> $profile_anomaly_batch_score_filename

sed -ie "s/<PWD>/$pwd/g" $profile_anomaly_batch_score_filename
rm -rf $profile_anomaly_batch_score_filename"e"

rm -rf $filename"e"
echo "Model Deployment for "$tenant_id" - ended"