from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

spark = SparkSession.builder.master("local[*]").appName("test").config("spark.jars","D:\\BigData\\spark-3.1.2-bin-hadoop3.2\\jars\\spark-snowflake_2.12-2.11.0-spark_3.1.jar").getOrCreate()

sc=spark.sparkContext
sc._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(sc._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())


# You might need to set these
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAQUILTBO3TT2C76ZA")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")

# Set options below
sfOptions = {
  "sfURL" : "lu31683.ap-southeast-1.snowflakecomputing.com",
  "sfUser" : "AJMERATEJAS2",
  "sfPassword" : "Tejas123#",
  "sfDatabase" : "SNOWFLAKE_SAMPLE_DATA",
  "sfSchema" : "TPCH_SF1",
  "sfWarehouse" : "SMALLWH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("query",  "select * from CUSTOMER") \
  .load()

df.show()
