from os.path import abspath
import subprocess,sys,os
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
        
    print("==========================================1=")
    spark.sql("use myDB")
    print("==========================================2=")
    spark.sql("SELECT * FROM txn").show()
    print("==========================================5=")
    sqlDF = spark.sql("SELECT key, value FROM txn WHERE key < 100 ORDER BY key")
    print("==========================================6=")
    sqlDF.coalesce(1).write.format("csv").save("gs://ravinuodb-bucket/output/txn-300/")
    print("==========================================7=")
    sqlDF = spark.sql("SELECT key, count(value) FROM txn Group BY key ORDER BY key")
    print("==========================================8=")
    sqlDF.coalesce(1).write.format("csv").save("gs://ravinuodb-bucket/output/txn-cnt300/")
    print("==========================================10.copy data==")
    spark.stop()
