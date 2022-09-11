from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("sparkDf").getOrCreate()
data = "D:\\BigData\\datasets\\donations1.csv"
rdd = spark.sparkContext.textFile(data)

odata =rdd.zipWithIndex().filter(lambda x:x[1]>3).map(lambda x :x[0].split(",")).toDF(["name","dt","amount"])
odata.createOrReplaceTempView("Tab")
#res=spark.sql("select * from Tab where amount=7000  name='venu'")
res=odata.where((col("name")=='venu') | (col("amount")>5000))
#res=odata.option("header","True")
#df=spark.read.csv(odata,header=True)
#df =spark.read.format('csv').option("","").option("header","True").load(odata)
res.show()
#df.printSchema()