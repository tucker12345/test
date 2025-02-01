import pytest
from pyspark.sql import SparkSession
from project.Mytest import Mytest

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.appName("test").getOrCreate()
    )
    return spark

def test_Mytest(spark):
    n = Mytest.Mytest.Mytest_method(spark)
    assert n == 2
    
    