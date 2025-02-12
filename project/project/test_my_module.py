import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from project.my_module import ETL

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("TestApp").getOrCreate()

def test_bronze(spark):
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", StringType(), True),
        StructField("order_amount", FloatType(), True),
        StructField("status", StringType(), True)
    ])
    data = [
        (1, 123, "2023-01-01", 100.0, "pending"),
        (2, None, "2023-01-02", 200.0, "completed"),
        (3, 456, "2023-01-03", 300.0, "pending")
    ]
    df = ETL.bronze(spark, schema, data)
    result = df.collect()
    assert len(result) == 2
    assert result[0]["customer_id"] == 123
    assert result[1]["customer_id"] == 456

def test_silver(spark):
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", StringType(), True),
        StructField("order_amount", FloatType(), True),
        StructField("status", StringType(), True)
    ])
    data = [
        (1, 123, "2023-01-01", 100.0, "pending"),
        (2, 456, "2023-01-02", 200.0, "completed")
    ]
    df = spark.createDataFrame(data, schema)
    result_df = ETL.silver(df)
    result = result_df.collect()
    assert result[0]["order_amount"] == 130.0
    assert result[1]["order_amount"] == 200.0

def test_gold(spark):
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", StringType(), True),
        StructField("order_amount", FloatType(), True),
        StructField("status", StringType(), True)
    ])
    data = [
        (1, 123, "2023-01-01", 100.0, "pending"),
        (2, 123, "2023-01-02", 200.0, "completed"),
        (3, 456, "2023-01-03", 300.0, "pending")
    ]
    df = spark.createDataFrame(data, schema)
    result_df = ETL.gold(df)
    result = result_df.collect()
    assert len(result) == 2
    assert result[0]["sum(order_amount)"] == 300.0 or result[0]["sum(order_amount)"] == 300.0
    assert result[1]["sum(order_amount)"] == 300.0 or result[1]["sum(order_amount)"] == 300.0