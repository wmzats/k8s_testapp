# -------
from os.path import abspath
import subprocess,sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructType, StructField


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS myDB LOCATION 'gs://proteus-bucket/temp'")
    print("==========================================1=")
    spark.sql("use myDB")
    print("==========================================2=")
    spark.sql("CREATE TABLE IF NOT EXISTS txn (key INT, value STRING) USING HIVE")
    spark.stop()
