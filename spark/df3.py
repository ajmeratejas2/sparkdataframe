from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data = "D:\\BigData\\datasets\\us-500.csv"
df=spark.read.csv(data,header=True,inferSchema=True)
def func(st):
    if (st=="NY"):
        return " 30% OFF"
    elif (st=="CA"):
        return " 40% OFF"
    elif (st=="OH"):
        return " 50% OFF"
    else:
        return " 500/- OFF"

uf= udf(func)
#ndf=df
ndf=df.withColumn("offer",uf(col("state")))
ndf.show(truncate=False)
ndf.printSchema()