from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="D:\\BigData\\datasets\\zips.json"

df=spark.read.format("json").option("header","true").load(data)
#res=df.withColumnRenamed("_id","id").withColumn("loc",explode(col("loc")))
res=df.withColumn("lati",(col("loc")[0])).withColumn("long",(col("loc")[1])).withColumnRenamed("_id","id").drop(col("loc"))
#res.show(truncate=False)
#res.printSchema()
op="D:\\BigData\\datasets\\output\\resultjson1"
res.createOrReplaceTempView("tab")
ndf=spark.sql("select * from tab where state='CA'")
ndf.show()
#ndf.printSchema()
ndf.write.mode("append").format("csv").option("header","true").save("D:\\BigData\\datasets\\output\\resultjson1")