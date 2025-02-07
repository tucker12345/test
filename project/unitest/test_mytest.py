"""unit test .

This module provides:
- copute field count;
- calculate dataframe count.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql import functions as F
import pytest

from project.my_module import ETL

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_amount", FloatType(), True),  
    StructField("status", StringType(), True)
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
    """Spark setting.

    returns spark.
    """
    return SparkSession.builder.appName("ETL").getOrCreate()

def test_field_count(spark: SparkSession) -> None:
    """Define test case.

    2 cases.
    """
    input_df = spark.createDataFrame(data, schema)

    n = len(input_df.schema.fields)
    assert n == 5

def test_bronze(spark: SparkSession) -> None:
    """Define test case.

    2 cases.
    """
    bonze_df = ETL.bronze(spark, schema, data)
    assert bonze_df.count() == 9
    
def test_silver(spark: SparkSession) -> None:
    """Define test case.

    2 cases.
    """
    bonze_df = ETL.bronze(spark, schema, data)
    silver_df = ETL.silver(bonze_df)
    total_order_amount = silver_df.agg(F.sum("order_amount")).collect()[0][0]
    assert round(total_order_amount,2) == 250.75 + 150.00 + 500.00 + 100*1.3 + 800.10 + 700.00 + 200.00*1.3
    
def test_gold(spark: SparkSession) -> None:
    """Define test case.

    2 cases.
    """    
    bonze_df = ETL.bronze(spark, schema, data)
    silver_df = ETL.silver(bonze_df)
    gold_df = ETL.gold(silver_df)
    assert gold_df.count() == 4
    total_order_amount = gold_df.agg(F.sum("sum(order_amount)")).collect()[0][0]
    assert round(total_order_amount,2) == 250.75 + 150.00 + 500.00 + 100*1.3 + 800.10 + 700.00 + 200.00*1.3
