"""unit test .

This module provides:
- copute field count;
- calculate dataframe count.
"""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pytest

from project.my_module import MyTest


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Spark setting.

    returns spark.
    """
    return SparkSession.builder.appName("test").getOrCreate()

def test_mytest(spark: SparkSession) -> None:
    """Define test case.

    2 cases.
    """
    
    the_df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    
    n = MyTest.mytest_method(the_df)
    assert n == 2
    n = MyTest.mytest_method2(the_df)
    assert n == 2
