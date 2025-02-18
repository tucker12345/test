"""Provide an ETL class for processing data through different stages: bronze, silver, and gold.

Classes:
    ETL: A class that provides methods to process data through different stages.
"""

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.sql.types import StructType,StringType

spark = SparkSession.builder.appName("MyApp").getOrCreate()

class ETL:
    """ETL class provides methods to process data through different stages: bronze, silver, and gold.

    Methods:
        bronze(_spark: SparkSession, _schema: StructType, _data: list) -> DataFrame:
        silver(_df: DataFrame) -> DataFrame:
        gold(_df: DataFrame) -> DataFrame:

    """

    @classmethod
    def bronze(cls, _spark: SparkSession,_schema:StructType,_path:StringType) -> DataFrame:
        """Create a DataFrame from the provided data and schema, and filters out rows where 'customer_id' is null.

        Args:
            _spark (SparkSession): The Spark session to use for creating the DataFrame.
            _schema (StructType): The schema to apply to the DataFrame.
            _data (list): The data to populate the DataFrame.

        Returns:
            DataFrame: A DataFrame with rows where 'customer_id' is not null.

        """
        src_df = _spark.read.schema( _schema).option("header", "true").csv(_path)
        return src_df

    @classmethod
    def silver(cls, _df: DataFrame) -> DataFrame:
        """Adjust the 'order_amount' column in the DataFrame based on the 'status' column.

        If the 'status' is 'pending', the 'order_amount' is multiplied by 1.3. Otherwise,
        the 'order_amount' remains unchanged.

        Args:
            _df (DataFrame): Input DataFrame containing 'status' and 'order_amount' columns.

        Returns:
            DataFrame: A new DataFrame with the adjusted 'order_amount' column.

        """
        df_filtered = _df.filter(_df["customer_id"].isNotNull())
        return df_filtered.withColumn(
            "order_amount",
            when(col("status") == "pending", col("order_amount") * 1.3).otherwise(col("order_amount"))
        )

    @classmethod
    def gold(cls, _df: DataFrame,_path:StringType) -> DataFrame:
        """Aggregate the input DataFrame by summing the order amounts for each customer.

        Args:
            _df (DataFrame): Input DataFrame containing customer orders.

        Returns:
            DataFrame: DataFrame with the total order amount for each customer, grouped by customer_id.

        """
        df_gold = _df.groupBy("customer_id").sum("order_amount")
        
        df_gold.write.mode("overwrite").parquet(_path)
        return df_gold
