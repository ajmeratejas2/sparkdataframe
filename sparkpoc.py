from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("sparkDf").getOrCreate()
data = "D:\\BigData\\datasets\\donations1.csv"
rdd=spark.sparkContext.textFile(data)
skip=rdd.first()
odata=rdd.filter(lambda x:x!=skip)
res = spark.read.csv(odata,header=True,inferSchema=True)
res.show()
res.printSchema()
#rdd = spark.sparkContext.textFile(data)
#odata=rdd.zipWithIndex().filter(lambda x:x[1]>2).map(lambda x :x[0].split(","))
#res =spark.read.csv(odata,header=True)
#res.show()
#for i in odata.collect():
    #print(i)