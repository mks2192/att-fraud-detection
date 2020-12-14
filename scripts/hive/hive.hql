set hive.support.sql11.reserved.keywords=false;

-- create the database for demo if it does not exist

CREATE DATABASE IF NOT EXISTS `demo`;

-- create table in the demo database for storing the profile anomaly score of entities and users for wgtraffic

CREATE TABLE IF NOT EXISTS demo.wgtraffic_profile_score ( `name` string, `type` string, `time_window` string, `timestamp` bigint, `pas_kmeans` double, `pas_isolation` double, `pas_svm` double, `pas` double) PARTITIONED BY ( `y` int, `m` int, `d` int) STORED AS ORC LOCATION '/user/elysium/demo/models/wgtraffic/profile_score';

-- create table in the demo database for storing the event anomaly score of entities and users for wgtraffic

CREATE TABLE IF NOT EXISTS demo.wgtraffic_event_alert_score ( `guid` string, `timestamp` bigint, `user_name` string, `entity_name` string, `source_type` string, `alerts` string, `alert_score` double, `event_score` double) PARTITIONED BY ( `y` int, `m` int, `d` int) STORED AS ORC LOCATION '/user/elysium/demo/models/wgtraffic/event_alert_score';

-- create table in the demo database for storing the profile anomaly score of entities and users for bluecoat

CREATE TABLE IF NOT EXISTS demo.bluecoat_profile_score ( `name` string, `type` string, `time_window` string, `timestamp` bigint, `pas_kmeans` double, `pas_isolation` double, `pas_svm` double, `pas` double) PARTITIONED BY ( `y` int, `m` int, `d` int) STORED AS ORC LOCATION '/user/elysium/demo/models/bluecoat/profile_score';

-- create table in the demo database for storing the event anomaly score of entities and users for bluecoat

CREATE TABLE IF NOT EXISTS demo.bluecoat_event_alert_score ( `guid` string, `timestamp` bigint, `user_name` string, `entity_name` string, `source_type` string, `alerts` string, `alert_score` double, `event_score` double) PARTITIONED BY ( `y` int, `m` int, `d` int) STORED AS ORC LOCATION '/user/elysium/demo/models/bluecoat/event_alert_score';

-- create table in the demo database for storing the aggregated anomaly score of entities and users across the data sources

CREATE TABLE IF NOT EXISTS demo.anomaly_score ( `name` string, `type` string, `time_window` string, `timestamp` bigint,as_wgtraffic double,as_bluecoat double,score double) PARTITIONED BY ( `y` int, `m` int, `d` int) STORED AS ORC LOCATION '/user/elysium/demo/models/anomaly_score';
