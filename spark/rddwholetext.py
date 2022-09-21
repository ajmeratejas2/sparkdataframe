from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
data= "D:\\BigData\\drivers-20220726T155103Z-001\\drivers\\bank-full.csv"
rdd=sc.wholeTextFiles(data)
drdd=sc.textFile(data)


for i in df.collect():
    print(i)

for i in drdd.collect():
    print(i)
