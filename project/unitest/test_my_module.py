# import csv
import io
import os
import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import functions as f
from project.my_module import ETL

csv_data = """order_id,customer_id,order_date,order_amount,status
1,1001,2025-02-01,250.75,completed
2,None,2025-02-02,320.50,pending
3,1001,2025-02-03,150.00,completed
4,1003,2025-02-04,0.00,canceled
5,1002,2025-02-05,None,completed
6,1004,2025-02-06,500.00,completed
7,1003,2025-02-07,100.00,pending
8,1004,2025-02-08,800.10,completed
9,1002,2025-02-09,700.00,completed
10,1001,2025-02-10,200.00,pending
"""

def IsTargetMatch(_df,_stage):
  try:
    flag = True
    if _stage == "bronze":
      # For order_id column
      _df.select("order_id").collect()[0][0] = 1
      _df.select("order_id").collect()[1][0] = 2
      _df.select("order_id").collect()[2][0] = 3
      _df.select("order_id").collect()[3][0] = 4
      _df.select("order_id").collect()[4][0] = 5
      _df.select("order_id").collect()[5][0] = 6
      _df.select("order_id").collect()[6][0] = 7
      _df.select("order_id").collect()[7][0] = 8
      _df.select("order_id").collect()[8][0] = 9
      _df.select("order_id").collect()[9][0] = 10
      
      # For customer_id column
      _df.select("customer_id").collect()[0][0] = 1001
      _df.select("customer_id").collect()[1][0] = None
      _df.select("customer_id").collect()[2][0] = 1001
      _df.select("customer_id").collect()[3][0] = 1003
      _df.select("customer_id").collect()[4][0] = 1002
      _df.select("customer_id").collect()[5][0] = 1004
      _df.select("customer_id").collect()[6][0] = 1003
      _df.select("customer_id").collect()[7][0] = 1004
      _df.select("customer_id").collect()[8][0] = 1002
      _df.select("customer_id").collect()[9][0] = 1001
      
      # For order_date column
      _df.select("order_date").collect()[0][0] = '2025-02-01'
      _df.select("order_date").collect()[1][0] = '2025-02-02'
      _df.select("order_date").collect()[2][0] = '2025-02-03'
      _df.select("order_date").collect()[3][0] = '2025-02-04'
      _df.select("order_date").collect()[4][0] = '2025-02-05'
      _df.select("order_date").collect()[5][0] = '2025-02-06'
      _df.select("order_date").collect()[6][0] = '2025-02-07'
      _df.select("order_date").collect()[7][0] = '2025-02-08'
      _df.select("order_date").collect()[8][0] = '2025-02-09'
      _df.select("order_date").collect()[9][0] = '2025-02-10'
      
      # For order_amount column
      _df.select("order_amount").collect()[0][0] = 250.75
      _df.select("order_amount").collect()[1][0] = 320.50
      _df.select("order_amount").collect()[2][0] = 150.00
      _df.select("order_amount").collect()[3][0] = 0.00
      _df.select("order_amount").collect()[4][0] = None
      _df.select("order_amount").collect()[5][0] = 500.00
      _df.select("order_amount").collect()[6][0] = 100.00
      _df.select("order_amount").collect()[7][0] = 800.10
      _df.select("order_amount").collect()[8][0] = 700.00
      _df.select("order_amount").collect()[9][0] = 200.00
      
      # For status column
      _df.select("status").collect()[0][0] = 'completed'
      _df.select("status").collect()[1][0] = 'pending'
      _df.select("status").collect()[2][0] = 'completed'
      _df.select("status").collect()[3][0] = 'canceled'
      _df.select("status").collect()[4][0] = 'completed'
      _df.select("status").collect()[5][0] = 'completed'
      _df.select("status").collect()[6][0] = 'pending'
      _df.select("status").collect()[7][0] = 'completed'
      _df.select("status").collect()[8][0] = 'completed'
      _df.select("status").collect()[9][0] = 'pending'
    elif _stage == "silver":
      # For order_id column
      _df.select("order_id").collect()[0][0] = 1
      # _df.select("order_id").collect()[1][0] = 2
      _df.select("order_id").collect()[2][0] = 3
      _df.select("order_id").collect()[3][0] = 4
      _df.select("order_id").collect()[4][0] = 5
      _df.select("order_id").collect()[5][0] = 6
      _df.select("order_id").collect()[6][0] = 7
      _df.select("order_id").collect()[7][0] = 8
      _df.select("order_id").collect()[8][0] = 9
      _df.select("order_id").collect()[9][0] = 10
      
      # For customer_id column
      _df.select("customer_id").collect()[0][0] = 1001
      # _df.select("customer_id").collect()[1][0] = None
      _df.select("customer_id").collect()[2][0] = 1001
      _df.select("customer_id").collect()[3][0] = 1003
      _df.select("customer_id").collect()[4][0] = 1002
      _df.select("customer_id").collect()[5][0] = 1004
      _df.select("customer_id").collect()[6][0] = 1003
      _df.select("customer_id").collect()[7][0] = 1004
      _df.select("customer_id").collect()[8][0] = 1002
      _df.select("customer_id").collect()[9][0] = 1001
      
      # For order_date column
      _df.select("order_date").collect()[0][0] = '2025-02-01'
      # _df.select("order_date").collect()[1][0] = '2025-02-02'
      _df.select("order_date").collect()[2][0] = '2025-02-03'
      _df.select("order_date").collect()[3][0] = '2025-02-04'
      _df.select("order_date").collect()[4][0] = '2025-02-05'
      _df.select("order_date").collect()[5][0] = '2025-02-06'
      _df.select("order_date").collect()[6][0] = '2025-02-07'
      _df.select("order_date").collect()[7][0] = '2025-02-08'
      _df.select("order_date").collect()[8][0] = '2025-02-09'
      _df.select("order_date").collect()[9][0] = '2025-02-10'
      
      # For order_amount column
      _df.select("order_amount").collect()[0][0] = 250.75
      # _df.select("order_amount").collect()[1][0] = 320.50
      _df.select("order_amount").collect()[2][0] = 150.00
      _df.select("order_amount").collect()[3][0] = 0.00
      _df.select("order_amount").collect()[4][0] = None
      _df.select("order_amount").collect()[5][0] = 500.00
      _df.select("order_amount").collect()[6][0] = 100.00 * 1.3
      _df.select("order_amount").collect()[7][0] = 800.10
      _df.select("order_amount").collect()[8][0] = 700.00
      _df.select("order_amount").collect()[9][0] = 200.00 * 1.3
      
      # For status column
      _df.select("status").collect()[0][0] = 'completed'
      # _df.select("status").collect()[1][0] = 'pending'
      _df.select("status").collect()[2][0] = 'completed'
      _df.select("status").collect()[3][0] = 'canceled'
      _df.select("status").collect()[4][0] = 'completed'
      _df.select("status").collect()[5][0] = 'completed'
      _df.select("status").collect()[6][0] = 'pending'
      _df.select("status").collect()[7][0] = 'completed'
      _df.select("status").collect()[8][0] = 'completed'
      _df.select("status").collect()[9][0] = 'pending'
    elif _stage == "gold":
      _df.select("customer_id").collect()[0][0] = 1001
      _df.select("customer_id").collect()[3][0] = 1003
      _df.select("customer_id").collect()[4][0] = 1002
      _df.select("customer_id").collect()[5][0] = 1004
      _df.select("order_amount").collect()[0][0] = 250.75
      _df.select("order_amount").collect()[1][0] = 320.50
      _df.select("order_amount").collect()[2][0] = 150.00
      _df.select("order_amount").collect()[3][0] = 0.00
      
  except Exception as e:
    flag = False
    print(f"An IsTargetMatch error occurred: {e}")
  finally:
    return flag

schema = StructType([
      StructField("order_id", IntegerType(), False),
      StructField("customer_id", IntegerType(), True),
      StructField("order_date", StringType(), True),
      StructField("order_amount", FloatType(), True),
      StructField("status", StringType(), True)
  ])

file = io.StringIO(csv_data)
path = os.getcwd()

if os.path.exists('sorce.csv'):
    os.remove('sorce.csv')

pdf = pd.read_csv(file, index_col=0)
pdf.to_csv('sorce.csv')
required_columns = ['order_id', 'customer_id', 'order_date','order_amount','status']

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return SparkSession.builder.appName("ETL").getOrCreate()

def test_bronze(spark: SparkSession) -> None:
    bonze_df = ETL.bronze(spark,schema, f"file:{path}/sorce.csv")
    missing_columns = [col for col in required_columns if col not in bonze_df.columns]
    
    # row by row test
    assert IsTargetMatch(bonze_df,"bronze")
    # count test
    assert bonze_df.count() == 10
    # schecma test
    assert not missing_columns

def test_silver(spark: SparkSession) -> None:
    bonze_df = ETL.bronze(spark, csv_data)
    silver_df = ETL.silver(bonze_df)
    missing_columns = [col for col in required_columns if col not in silver_df.columns]
    
    # row by row test
    assert IsTargetMatch(bonze_df,"silver_df")
    # count test
    assert silver_df.count() == 9
    # schecma test
    assert not missing_columns    

def test_gold(spark: SparkSession) -> None:
    bonze_df = ETL.bronze(spark, csv_data)
    silver_df = ETL.silver(bonze_df)
    gold_df = ETL.gold(silver_df,f"file:{path}/dest.parquet") 
    missing_columns = [col for col in ['customer_id', 'order_amount'] if col not in gold_df.columns]
    
    # row by row test
    assert IsTargetMatch(gold_df,"gold")
    # count test
    assert gold_df.count() == 4
    # schecma test
    assert not missing_columns
    # aggregation test
    total_order_amount = gold_df.agg(f.sum("sum(order_amount)")).collect()[0][0]
    assert round(total_order_amount,2) == 250.75 + 150.00 + 500.00 + 100*1.3 + 800.10 + 700.00 + 200.00*1.3
