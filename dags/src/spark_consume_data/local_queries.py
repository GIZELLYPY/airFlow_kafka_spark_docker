from pyspark.sql import SparkSession
import os
from pyspark.sql import DataFrame
from airflow.models import Variable

DATA_OUTPUT = Variable.get("data_output")

output_dir = os.getcwd() + DATA_OUTPUT


def taxi_ride_local_per_hour(spark: "SparkSession") -> DataFrame:
    """Returns a DataFrame representing the result of the given query.
    This query calculate how many taxi ride there are per hour considering
    same local. The Dataframe is saved on data lake partitioned by columns
    [pickup year] and [pickup year month].

    Args:
        spark (SparkSession): Spark's session

    Returns:
        DataFrame: Dataframe processed by query.
    """
    spark = spark
    sql = """
    SELECT
    pickup_year,
    pickup_year_month,
    pickup_date,
    pickup_hour,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    count(*) as total_ride,
    avg(fare_amount) as average_fare_amount,
    sum(fare_amount) as total_fare_amount
    FROM TAXI_TRANSACTIONS
    GROUP BY
    pickup_year,
    pickup_year_month,
    pickup_date,
    pickup_hour,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude
    ORDER BY total_ride DESC
    """
    taxi_ride_local_per_hour = spark.sql(sql)
    taxi_ride_local_per_hour.write.option("header", True).partitionBy(
        ["pickup_year", "pickup_year_month"]
    ).mode("overwrite").parquet(
        output_dir + "analytics/taxi_ride_local_per_hour.parquet"
    )


def taxi_ride_local(spark: "SparkSession") -> DataFrame:
    """Returns a DataFrame representing the result of the given query.
    This query calculate how many taxi ride there are per month considering
    same local. The Dataframe is saved on data lake partitioned by columns
    [pickup year] and [pickup year month].

    Args:
        spark (SparkSession): Spark's session

    Returns:
        DataFrame: Dataframe processed by query.
    """
    spark = spark
    sql = """ SELECT pickup_year,
    pickup_year_month,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    count(*) as total_ride,
    avg(fare_amount) as average_fare_amount,
    sum(fare_amount) as total_fare_amount
    FROM TAXI_TRANSACTIONS
    GROUP BY
    pickup_year,
    pickup_year_month,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude
    ORDER BY total_ride DESC
    """

    taxi_ride_local = spark.sql(sql)
    taxi_ride_local.write.option("header", True).partitionBy(
        ["pickup_year", "pickup_year_month"]
    ).mode("overwrite").parquet(output_dir +
                                "analytics/taxi_ride_local.parquet")


def taxi_ride_local_ranking(spark: "SparkSession") -> DataFrame:
    """Returns a DataFrame representing the result of the given query.
    This query calculate how many taxi ride there are considering
    same local. The Dataframe is saved on data lake partitioned by columns
    [pickup year] and [pickup year month].

    Args:
        spark (SparkSession): Spark's session

    Returns:
        DataFrame: Dataframe processed by query.
    """
    spark = spark
    sql = """ SELECT
            pickup_year,
            pickup_year_month,
            pickup_longitude,
            pickup_latitude,
            dropoff_longitude,
            dropoff_latitude,
            count(*) as total_ride,
            avg(fare_amount) as average_fare_amount,
            sum(fare_amount) as total_fare_amount
            FROM TAXI_TRANSACTIONS
            GROUP BY
            pickup_year,
            pickup_year_month,
            pickup_longitude,
            pickup_latitude,
            dropoff_longitude,
            dropoff_latitude
            ORDER BY total_ride DESC"""

    taxi_ride_local_ranking = spark.sql(sql)
    taxi_ride_local_ranking.write.option("header", True).partitionBy(
        ["pickup_year", "pickup_year_month"]
    ).mode("overwrite").parquet(
        output_dir + "analytics/taxi_ride_local_ranking.parquet"
    )


def main():
    spark = (
        SparkSession.builder.appName("NEW_YORK_TAXI_PLACES")
        .enableHiveSupport()
        .getOrCreate()
    )

    taxi_ride_local_per_hour(spark)
    taxi_ride_local(spark)
    taxi_ride_local_ranking(spark)


if __name__ == "__main__":
    main()
