from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="D:\\BigData\\datasets\\cust_spent.csv"
df=spark.read.format("csv").option("header","True").option("inferSchema","True").load(data)
res=df.map
#res=df.groupBy(col("CustName")).agg(count(col("*")).alias("cnt")).orderBy(col("cnt"))
#res=df.groupby(col("CustName")).agg(sum(col("SpentMoney")))
#res.show()
#df.show()
#df.printSchema()
