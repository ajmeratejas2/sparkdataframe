from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark .readStream .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "indpak") \
  .load()

#ndf=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
query = df .writeStream.format("console")  .start()

query.awaitTermination()