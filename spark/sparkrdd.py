from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
data= "D:\\BigData\\datasets\\donations1.csv"
rdd =sc.textFile(data)
skip=rdd.first()
res=rdd.filter(lambda x:x!=skip).map(lambda x:x.split(",")).filter(lambda x: "dt" not in x).map(lambda x:(x[0],int(x[2])))\
.reduceByKey(lambda x,y:x+y).sortByKey(lambda x:x[0])

for i in res.collect():
    print(i)
