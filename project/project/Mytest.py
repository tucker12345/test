# import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()

class Mytest():
    
    @classmethod
    def Mytest_method(cls,_spark):
        df = _spark.createDataFrame([(1, 'Alice'), (2, 'Bob')], ['id', 'name'])
        return len(df.schema.fields)

    @classmethod
    def Mytest_method2(cls,_spark):
        df = _spark.createDataFrame([(1, 'Alice'), (2, 'Bob')], ['id', 'name'])
        return df.count()
    
# df = spark.createDataFrame([(1, 'Alice'), (2, 'Bob')], ['id', 'name'])    
# df.printSchema()    