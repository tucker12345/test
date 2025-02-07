"""test classes.

This module provides:
- copute field count;
- calculate dataframe count.
"""

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()


class MyTest:
    """MyTest class.

    Define 2 methods.
    """

    @classmethod
    def mytest_method(cls, _df: DataFrame) -> int:
        """_spark current spark.

        returns length of fields.
        """
        return len(_df.schema.fields)

    @classmethod
    def mytest_method2(cls, _df: DataFrame) -> int:
        """_spark current spark.

        returns dataframe count.
        """
        return _df.count()
