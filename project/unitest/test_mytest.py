
"""Test suite for the ETL process using PySpark and pytest.

This test suite includes the following tests:
1. test_field_count: Verifies that the input DataFrame has the expected number of fields.
2. test_bronze: Tests the ETL.bronze function to ensure it processes the input data correctly.
3. test_silver: Tests the silver transformation of the ETL process to verify correct aggregation of order amounts.
4. test_gold: Tests the entire ETL pipeline from bronze to gold dataframes and verifies the final output.
Fixtures:
- spark: Creates and returns a SparkSession with the application name "ETL".
Tests:
- test_field_count(spark: SparkSession) -> None: Asserts that the number of fields in the DataFrame schema is equal to 5.
- test_bronze(spark: SparkSession) -> None: Asserts that the resulting DataFrame from ETL.bronze has 9 rows.
- test_silver(spark: SparkSession) -> None: Asserts that the total order amount in the silver DataFrame matches the expected value.
- test_gold(spark: SparkSession) -> None: Asserts that the gold DataFrame contains exactly 4 rows and the total order amount
matches the expected value.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
import pytest

from project.my_module import ETL

schema = StructType([
    StructField("order_id", IntegerType(), nullable=True),
    StructField("customer_id", IntegerType(), nullable=True),
    StructField("order_date", StringType(), nullable=True),
    StructField("order_amount", FloatType(), nullable=True),
    StructField("status", StringType(), nullable=True)
])

data = [
    (1, 1001, "2025-02-01", 250.75, "completed"),
    (2, None, "2025-02-02", 320.50, "pending"),
    (3, 1001, "2025-02-03", 150.00, "completed"),
    (4, 1003, "2025-02-04",  0.00, "canceled"),
    (5, 1002, "2025-02-05", None, "completed"),
    (6, 1004, "2025-02-06", 500.00, "completed"),
    (7, 1003, "2025-02-07", 100.00, "pending"),
    (8, 1004, "2025-02-08", 800.10, "completed"),
    (9, 1002, "2025-02-09", 700.00, "completed"),
    (10, 1001, "2025-02-10", 200.00, "pending")]

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Create and return a SparkSession with the application name "ETL".

    Returns:
        SparkSession: An active SparkSession object.

    """
    return SparkSession.builder.appName("ETL").getOrCreate()

def test_field_count(spark: SparkSession) -> None:
    """Test that the input DataFrame has the expected number of fields.

    Args:
        spark (SparkSession): The Spark session used to create the DataFrame.
    Asserts:
        The number of fields in the DataFrame schema is equal to 5.

    """
    input_df = spark.createDataFrame(data, schema)

    n = len(input_df.schema.fields)
    assert n == 5

def test_bronze(spark: SparkSession) -> None:
    """Test the ETL.bronze function.

    This test checks if the ETL.bronze function correctly processes the input data
    and returns a DataFrame with the expected number of rows.

    Args:
        spark (SparkSession): The SparkSession object used to create DataFrames.

    Asserts:
        The resulting DataFrame from ETL.bronze has 9 rows.

    """
    bonze_df = ETL.bronze(spark, schema, data)
    assert bonze_df.count() == 9

def test_silver(spark: SparkSession) -> None:
    """Test the silver transformation of the ETL process.

    This test verifies that the silver transformation correctly aggregates the order amounts
    from the bronze DataFrame and matches the expected total order amount.

    Args:
        spark (SparkSession): The Spark session used for creating DataFrames.

    Raises:
        AssertionError: If the total order amount in the silver DataFrame does not match the expected value.

    """
    bonze_df = ETL.bronze(spark, schema, data)
    silver_df = ETL.silver(bonze_df)
    total_order_amount = silver_df.agg(f.sum("order_amount")).collect()[0][0]
    assert round(total_order_amount,2) == 250.75 + 150.00 + 500.00 + 100*1.3 + 800.10 + 700.00 + 200.00*1.3

def test_gold(spark: SparkSession) -> None:
    """Test the ETL pipeline from bronze to gold dataframes.

    This test performs the following steps:
    1. Creates a bronze dataframe using the ETL.bronze method.
    2. Transforms the bronze dataframe to a silver dataframe using the ETL.silver method.
    3. Transforms the silver dataframe to a gold dataframe using the ETL.gold method.
    4. Asserts that the gold dataframe contains exactly 4 rows.
    5. Aggregates the total order amount from the gold dataframe and asserts that it matches the expected value.

    Args:
        spark (SparkSession): The Spark session to use for creating dataframes.

    Raises:
        AssertionError: If the number of rows in the gold dataframe is not 4 or if the total order amount does not match the expected value.

    """
    bonze_df = ETL.bronze(spark, schema, data)
    silver_df = ETL.silver(bonze_df)
    gold_df = ETL.gold(silver_df)
    assert gold_df.count() == 4
    total_order_amount = gold_df.agg(f.sum("sum(order_amount)")).collect()[0][0]
    assert round(total_order_amount,2) == 250.75 + 150.00 + 500.00 + 100*1.3 + 800.10 + 700.00 + 200.00*1.3
