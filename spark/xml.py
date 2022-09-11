from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="D:\\BigData\\datasets\\books.xml"
df=spark.read.format("xml").option("rowtag","book").load(data)
res=df.withColumnRenamed("_id","id").filter(col("price")>30)
op="D:\\BigData\\datasets\\output\\temp.csv"

res.write.mode("overwrite").format("xml").option("rowTag", "book")\
    .save("D:\BigData\datasets\output")
res.show()
import findspark
findspark.init()

#res.write.mode("overwrite").format("csv").option("header","True").save(op)
#df.show()

