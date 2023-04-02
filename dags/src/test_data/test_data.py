from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import json
import logging
from airflow.models import Variable

great_expectation_suite_path = Variable.get("TEST_SUITE")


def get_data(spark: "SparkSession") -> DataFrame:
    """Collect data of the given query. Returns a dataset able to be used with
    the library Great Expectations.


    Args:
        spark (SparkSession): Spark's session

    Returns:
        DataFrame: Dataframe processed by query.
    """
    spark = spark
    sql = """
    SELECT * FROM TAXI_TRANSACTIONS
    """
    df = spark.sql(sql)
    great_df = SparkDFDataset(df)
    return great_df


def load_expectation_suite(great_expectation_suite_path: str) -> dict:
    """Load expectation suite stored in JSON format
    and convert into dictionary.
    Args:
        path (str): path to expectation suite json file
    Returns:
        dict: expectation suite
    """
    with open(great_expectation_suite_path, "r") as f:
        expectation_suite = json.load(f)
    return expectation_suite


def great_expectation_validation(df: DataFrame, expectation_suite_path: str) -> dict:
    """Run validation on DataFrame based on expecation suite
    Args:
        df pyspark.sql.DataFrame: DataFrame to validate
        expectation_suite_path str: path to expectation suite json file
    Returns:
        dict: Validation result
    """
    expectation_suite = load_expectation_suite(expectation_suite_path)

    validation_results = df.validate(
        expectation_suite=expectation_suite,
        result_format="SUMMARY",
        catch_exceptions=True,
    )
    print(validation_results["success"])

    return validation_results["success"]


def main():
    spark = (
        SparkSession.builder.appName("NEW_YORK_TAXI_TEST_DATA")
        .enableHiveSupport()
        .getOrCreate()
    )

    great_df = get_data(spark)

    logging.info("Starting data validation ...")

    validation_result = great_expectation_validation(
        df=great_df,
        expectation_suite_path=great_expectation_suite_path,
    )

    return validation_result


if __name__ == "__main__":
    main()
