from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="D:\\BigData\\datasets\\us-500.csv"
df=spark.read.format("csv").option("inferschema","true").option("header","true").load(data)
#ndf=df.withColumn("age",lit(18))
ndf=df.withColumn("fullname",concat_ws("_",col("first_name"),col("last_name")))\
    .withColumn("emil",substring_index("email","@",1)).withColumn("emailid",substring_index("email","@",-1))\
    .withColumn("state",when(col("state")=="NY","NEWYORK").when(col("state")=="OH","OHIYO").otherwise(col("state")))
ndf.show(truncate=False)
ndf.printSchema()