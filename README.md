# Elysium Models

# Clone the repo (in all the machines)
```bash
git clone http://gitlab.sstech.mobi/vshlyakhtin/sia.git
```

# Switch to ATT-1.8.0-Models branch

```bash
cd sia
git checkout ATT-1.8.0-Models
cd ..
```

# To install Python 3.6.5 and all pip dependencies in the spark cluster (in all the machines)

```bash
sia/models/scripts/install_python.sh
```
- To validate
    - Issue following command to check python version:-

        ```bash
        $ /usr/local/bin/python3.6 
        Python 3.6.5 (default, Jan 21 2019, 11:33:38) 
        [GCC 4.8.5 20150623 (Red Hat 4.8.5-36)] on linux
        Type "help", "copyright", "credits" or "license" for more information.
        ```
    - Issue following command to check pip3 version:-

        ```bash
        $ pip3 --version
        pip 9.0.3 from /usr/local/lib/python3.6/site-packages (python 3.6)
        ```
        
#To run the project (from Master Node)


## To configure the config file for the project (One time while deploying the project)

```bash
cd sia/models
./scripts/deploy_models.sh demo /tmp info error ip_src_addr,ip_dst_addr,ip_src_port,ip_dst_port in_bytes,out_bytes \#att-models-log xoxb-501492022263-574304627685-xkt9wroHCGig8NQhZyJYinkt 10.10.150.21 8990 dev "{\"is_nu\":14,\"is_ne\":5,\"is_uatone\":15,\"is_uafromne\":4,\"is_nwpu\":16,\"is_nwpp\":11,\"is_nactpu\":10,\"is_nactpp\":17,\"is_nacnpu\":19,\"is_nacnpp\":1}" 10.10.150.21:6667,10.10.150.22:6667,10.10.150.23:6667 indexing /tmp/checkpoint wgtraffic,bluecoat  null updnratio r2kafka
cd ../..
```

## Initialize hive tables (One time after every deployment)

```bash
hive -f sia/models/scripts/hive/hive.hql
```

## To Train: Profile based ML models (Scheduled in regular interval; Should be part of crontab)

```bash
sia/models/scripts/cron/profile_anomaly_train.sh

```

## To Train: Event based ML models (Scheduled in regular interval; Should be part of crontab)

```bash
sia/models/scripts/cron/event_anomaly_train.sh

```

## To Score : Spark Streaming job for event scoring (One time after every deployment; Should be run as background job)

```bash
nohup sia/models/scripts/cron/event_anomaly_online_score.sh > nohup_event_anomaly_online_score.out&
```

## To Score : Batch job for profile scoring and final aggregation of scores (Scheduled in regular interval; Should be part of crontab)

```bash
sia/models/scripts/cron/profile_anomaly_batch_score.sh
```

