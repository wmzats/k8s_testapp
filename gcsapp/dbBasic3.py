from os.path import abspath
import subprocess,sys,os
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructType, StructField
from distutils.dir_util import copy_tree


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .enableHiveSupport() \
        .getOrCreate()
        
    print("==========================================0.download package from bucket=")
    os.system('gcloud auth activate-service-account infra-build@aml-innov-bpid-810834.iam.gserviceaccount.com --key-file=/opt/spark/examples/src/main/python/infra-build.json')
    os.system('gsutil cp -r gs://ravinuodb-bucket/temp/myapp /home/infra-build')    
    print("==========================================1=")
    spark.sql("use myDB")
    print("==========================================2=")
    spark.sql("SELECT * FROM txn").show()
    print("==========================================5=")
    sqlDF = spark.sql("SELECT key, value FROM txn WHERE key < 100 ORDER BY key")
    print("==========================================6=")
    sqlDF.coalesce(1).write.format("csv").save("gs://ravinuodb-bucket/output/txn-200/")
    print("==========================================7=")
    sqlDF = spark.sql("SELECT key, count(value) FROM txn Group BY key ORDER BY key")
    print("==========================================8=")
    sqlDF.coalesce(1).write.format("csv").save("gs://ravinuodb-bucket/output/txn-cnt200/")
    #df = spark.read.parquet("/home/infra-build/myapp/metastore_db")
    print("==========================================10.copy data==")
    #df.write.parquet("gs://ravinuodb-bucket/temp")
    #copy_tree("/home/infra-build/myapp/metastore_db","gs://ravinuodb-bucket/temp")
    os.system('gsutil cp -r /home/infra-build/myapp gs://ravinuodb-bucket/temp')
    spark.stop()
