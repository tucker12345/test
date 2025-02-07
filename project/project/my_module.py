"""test classes.

This module provides:
- copute field count;
- calculate dataframe count.
"""

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import when, col

spark = SparkSession.builder.appName("MyApp").getOrCreate()


class ETL:
    """MyTest class.

    Define 2 methods.
    """

    @classmethod
    def bronze(cls, _spark: SparkSession,_schema:StructType,_data:list) -> DataFrame:
        """_spark current spark.

        returns dataframe count.
        """
        src_df = _spark.createDataFrame(_data, _schema)
        return src_df.filter(src_df["customer_id"].isNotNull())

    @classmethod
    def silver(cls, _df: DataFrame) -> DataFrame:
        """_spark current spark.

        returns dataframe count.
        """
        df_silver = _df.withColumn(
            "order_amount",
            when(col("status") == "pending", col("order_amount") * 1.3).otherwise(col("order_amount"))
        )
        return df_silver

    @classmethod
    def gold(cls, _df: DataFrame) -> DataFrame:
        """_spark current spark.

        returns dataframe count.
        """    
        df_gold = _df.groupBy("customer_id").sum("order_amount")
        return df_gold