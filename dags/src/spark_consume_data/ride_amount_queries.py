from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import os
from airflow.models import Variable


DATA_OUTPUT = Variable.get("DATA_OUTPUT")
output_dir = DATA_OUTPUT


def ride_per_month(spark: "SparkSession") -> DataFrame:
    """Returns a DataFrame representing the result of the given query.
    This query calculates fare amount per month and year.
    The Dataframe is saved on data lake partitioned by columns
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
    count(*) as total_ride,
    avg(fare_amount) as average_fare_amount,
    sum(fare_amount) as total_fare_amount
    FROM TAXI_TRANSACTIONS
    GROUP BY pickup_year, pickup_year_month
    ORDER BY total_ride DESC
    """

    ride_per_month = spark.sql(sql)

    ride_per_month.write.option("header", True).partitionBy(
        ["pickup_year", "pickup_year_month"]
    ).mode("overwrite").parquet(output_dir + "analytics/taxi_fare_amount.parquet")


def ride_amount_per_hour(spark: "SparkSession") -> DataFrame:
    """Returns a DataFrame representing the result of the given query.
    This query calculates fare amount per hour.
    The Dataframe is saved on data lake partitioned by columns
    [pickup year] and [pickup year month].


    Args:
        spark (SparkSession): Spark's session

    Returns:
        DataFrame: Dataframe processed by query.
    """
    spark = spark
    sql = """
    SELECT pickup_year,
    pickup_year_month,
    pickup_date,
    pickup_hour,
    count(*) as total_ride,
    avg(fare_amount) as average_fare_amount,
    sum(fare_amount) as total_fare_amount
    FROM TAXI_TRANSACTIONS
    GROUP BY pickup_year, pickup_year_month, pickup_date, pickup_hour
    ORDER BY
    total_ride DESC
    """

    ride_amount_per_hour = spark.sql(sql)

    ride_amount_per_hour.write.option("header", True).partitionBy(
        ["pickup_year", "pickup_year_month"]
    ).mode("overwrite").parquet(
        output_dir + "analytics/taxi_fare_amount_per_hour.parquet"
    )


def main():
    spark = (
        SparkSession.builder.appName("NEW_YORK_TAXI_FARE_AMOUNT")
        .enableHiveSupport()
        .getOrCreate()
    )

    ride_per_month(spark)
    ride_amount_per_hour(spark)


if __name__ == "__main__":
    main()
