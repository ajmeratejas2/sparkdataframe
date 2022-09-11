from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
import re
data="D:\\BigData\\datasets\\2017.csv"
rdd=sc.textFile(data)
#res=rdd.map(lambda x:x.split(""))
res=rdd.flatMap(lambda x:x.split(""))

#df=spark.read.csv(data,header=True,inferSchema=True)
#df.show()
#df.printSchema()
#cols=[re.sub('[^0-9a-zA-Z]',"",c.lower()) for c in df.columns]
#res=df.toDF(*cols)
#res.show()
#res.printSchema()
for i in rdd.collect():
    print(i)

