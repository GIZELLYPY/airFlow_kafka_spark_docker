# AirFlow + Kafka + Spark + Docker
### Streaming application data using New York City Taxi Fare.
The data pipeline is shown in the following airflow DAG picture:

#### DAG: 1_streaming
Write events to a Kafka cluster. A producer partitioner maps each message from the train.csv file to a topic partition named by Transactions, and the producer sends a produce request to the leader of that partition. The partitioners shipped with Kafka guarantee that all messages with the same non-empty key will be sent to the same partition.

![Screenshot from 2023-04-02 14-45-12](https://user-images.githubusercontent.com/52425554/229369809-daa54a4f-a497-4295-9e47-960fbc4cecb0.png)

#### DAG: 2_consumer: 
- Create a path used to recover from failures if something goes wrong during data treatments
- Read (consume) messages from a number of Kafka TOPICS, in this case we have a unic topic called by: <code>Topic1 = "transactions"</code>
- Run validation (Data Test) on data based on expecation suite.
- If the Data Test is True, that means there is no problem with the data and this can be modeled and saved with success. But if the Data Test is False, the data will be discarded.

![Screenshot from 2023-04-02 15-26-14](https://user-images.githubusercontent.com/52425554/229371667-7ce219fc-25aa-40b4-a06a-1304ea56920c.png)


The data are modeled for be saved on this ways:  
- <code>ride_per_month</code> This query calculates fare amount per month and year.
    The Dataframe is saved on data lake partitioned by column pickup year and pickup year month.
- <code>ride_amount_per_hour</code>This query calculates fare amount per hour.
    The Dataframe is saved on data lake partitioned by columns
    pickup year and pickup year month.
- <code>taxi_ride_local_per_hour</code>This query calculate how many taxi ride there are per hour considering
    same local. The Dataframe is saved on data lake partitioned by columns pickup year and pickup year month.
- <code>taxi_ride_local</code>This query calculate how many taxi ride there are per month considering
    same local. The Dataframe is saved on data lake partitioned by columns pickup year and pickup year month.
- <code>taxi_ride_local_ranking</code>This query calculate how many taxi ride there are considering
    same local. The Dataframe is saved on data lake partitioned by columns pickup year and pickup year month.


![Screenshot from 2023-04-02 15-27-46](https://user-images.githubusercontent.com/52425554/229371727-243ae44f-fedd-4a84-b163-06b31780c7a0.png)
![image](https://user-images.githubusercontent.com/52425554/229371742-845331da-0ea8-41c9-9d45-f22c08883544.png)









## Docker Container based architecture:
- Container 1: Postgresql for Airflow db
- Container 2: Airflow + KafkaProducer
- Container 3: Zookeeper for Kafka server
- Container 4: Kafka Server
- Container 5: Spark + hadoop
- Container 2 is responsible for producing data in a stream fashion, so my source data (train.csv).
- Container 5 is responsible for Consuming the data and projecting the insights 
> To bind all the containers together using docker-compose i have taken Pre-Configured DockerFiles available on Docker-hub.


![image](https://user-images.githubusercontent.com/52425554/229369113-55aad4ca-cfaf-488f-bd11-88df3ba13f52.png)

## SETUP:
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
        * <code>source ~/.bashrc</code>
  - 4) Start Hive inside container:
        * Access the container using: <code>docker exec -ti [airflow-container-id] bash</code>
        * Before you run hive for the first time, run: <code>schematool -initSchema -dbType derby</code>
        * If you already ran hive and then tried to initSchema and it's failing:
            * <code>cd /data/hive/</code>
            * <code>mv metastore_db metastore_db.tmp</code>
            * Re run: <code>schematool -initSchema -dbType derby</code>
   - 5) <code>cd /opt/apache-hive-2.0.1-bin/bin/</code>
            * <code>chmod 777 hive</code>
            * <code>hive --service metastore</code>
  - 6) **Now trigger the DAG 1_streaming from Airflow UI.**
