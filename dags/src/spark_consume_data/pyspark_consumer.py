from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import sys
from time import sleep
import logging


def treat_transactions(input_df: DataFrame, checkpoint_path: str) -> DataFrame:
    """Treat input_df. Apply regex, replace  and rename columns
    for extract raw data and distribuit each column in each place as
    a regular DataFrame.

    Args:
        input_df (DataFrame): Raw DataFrame comming from data streaming.
        checkpoint_path (str): Used to recover from failures

    Returns:
        DataFrame: Regular Dataframe with renamed columns
    """

    treated_df = input_df.withColumn(
        "new_val", F.regexp_replace(input_df["value"], "\\\\", "")
    ).drop("value")
    treated_df = treated_df.withColumn(
        "value", F.regexp_replace(treated_df["new_val"], '""', "'")
    ).drop("new_val")
    treated_df = treated_df.withColumn(
        "new_val", F.regexp_replace(treated_df["value"], "}n", "}")
    ).drop("value")
    treated_df = treated_df.withColumn(
        "new_val", F.regexp_replace(treated_df["new_val"], "'", "")
    )

    split_col = F.split(treated_df["new_val"], ",")
    df = treated_df.withColumn("key", split_col.getItem(0))
    df = (
        df.withColumn("fare_amount", split_col.getItem(1))
        .withColumn("pickup_datetime", split_col.getItem(2))
        .withColumn("pickup_longitude", split_col.getItem(3))
        .withColumn("pickup_latitude", split_col.getItem(4))
        .withColumn("dropoff_longitude", split_col.getItem(5))
        .withColumn("dropoff_latitude", split_col.getItem(6))
        .withColumn("passenger_count", split_col.getItem(7))
    )
    df = df.drop(df.new_val)

    df = (
        df.withColumn("pickup_year", F.substring("pickup_datetime", 1, 4))
        .withColumn("pickup_year_month", F.substring("pickup_datetime", 1, 7))
        .withColumn("pickup_date", F.substring("pickup_datetime", 1, 10))
        .withColumn("hour", F.split(df.pickup_datetime, ":")[0])
    )

    df = df.withColumn("pickup_hour", F.split(df.hour, " ")[1])

    query = (
        df.writeStream.format("memory")
        .queryName("TransactionTable")
        .outputMode("append")
        .option("checkpoint", checkpoint_path)
        .option("includeHeaders", "true")
        .start()
    )

    query.awaitTermination(5)

    # Let it Fill up the table
    sleep(10)


def pyspark_consumer(
    checkpoint_trans_path: str, CLIENT: str, TOPICS: str, spark: "SparkSession"
) -> DataFrame:
    """Read messages from a number of Kafka TOPICS.

    Args:
        checkpoint_trans_path (str):  Used to recover from failures
        CLIENT (str): Kafka client
        TOPICS (str): Kafka topic
        spark (SparkSession): Spark's session

    Returns:
        DataFrame: Spark dataframe saved on memory as a Table
    """

    spark = spark
    checkpoint_trans_path = checkpoint_trans_path
    client = CLIENT
    topics = TOPICS

    trans_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", client)
        .option("subscribe", topics)
        .option("startingOffsets", "earliest")
        .load()
    )

    matchdata_transactions = trans_df.selectExpr(
        "CAST(value AS STRING) as value"
    ).filter("topic = 'transactions'")

    # Call transaction function
    treat_transactions(matchdata_transactions, checkpoint_trans_path)

    # Check if the dataframe is still streaming , if not then get out
    while trans_df.isStreaming:
        trans_df = spark.sql("select * from TransactionTable")
        trans_df.write.saveAsTable("TAXI_TRANSACTIONS", mode="overwrite")
        sleep(5)


def main():

    checkpoint_trans_path = sys.argv[1]
    CLIENT = sys.argv[2]
    TOPICS = sys.argv[3]

    spark = (
        SparkSession.builder.appName("Pyspark_consumer")
        .enableHiveSupport()
        .config("kafka.partition.assignment.strategy", "range")
        .getOrCreate()
    )

    logging.info(
        "Start to consume streaming data and saving on \
            memory as a table named by TAXI_TRANSACTIONS...."
    )
    pyspark_consumer(checkpoint_trans_path, CLIENT, TOPICS, spark)
    logging.info("Finishing consuming.")


if __name__ == "__main__":
    main()
