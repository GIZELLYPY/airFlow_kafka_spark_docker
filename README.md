# airFlow_kafka_spark_docker
Streaming application data 

Before starting any dag, it is necessary to do some settings. These settings are described below:
- 1) Download train.csv https://www.kaggle.com/competitions/new-york-city-taxi-fare-prediction/data?select=train.csv  and save on directory data.
- 2) On Airflow UI, create the airflow variables in Admin>Variables:
        * **BOOTSTRAP_SERVERS** = ["kafka:9092"]
        * **DATA_OUTPUT** = /data/output/
        * **TEST_SUITE** = /usr/local/airflow/data/great_expectation_suite.json
        * **PATH_STREAM** = /data/train.csv
 - 3) Export Java Home inside container: 
        * Access the container using: <code>docker exec -ti [airflow-container-id] bash</code>
        * <code>export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")</code>
        * <code>export source ~/.bashrc</code>
  - 4) Start Hive inside container:
        * Access the container using: <code>docker exec -ti [airflow-container-id] bash</code>
        * Before you run hive for the first time, run: <code>schematool -initSchema -dbType derby</code>
        * If you already ran hive and then tried to initSchema and it's failing:
            * <code>cd /data/hive/</code>
            * <code>mv metastore_db metastore_db.tmp</code>
            * Re run: <code>schematool -initSchema -dbType derby</code>
            * <code>cd /opt/apache-hive-2.0.1-bin/bin/</code>
            * <code>chmod 777 hive</code>
            * <code>hive --service metastore</code>
  - 5) **Now trigger the DAG 1_streaming from Airflow UI.**
   
