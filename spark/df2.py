from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data = "D:\\BigData\\datasets\\us-500.csv"
df=spark.read.csv(data,header=True,inferSchema=True)
ndf=df.groupby(col("state")).agg(count(col("*")).alias("cnt")).orderBy(col("cnt").desc())
#ndf=df.withColumn("age",lit(21)).withColumn("fullname",concat_ws(" ",col("first_name"),col("last_name")))\
#    .withColumn("phno1",regexp_replace(col("phone1"),"-","").cast(LongType()))
#ndf=df.groupBy(df.state).agg(countDistinct(col("city")).alias("cnt"),collect_list("city").alias("names")).orderBy(col("cnt"))
#ndf=df.groupBy(df.state).agg(countDistinct(col("city")).alias("cnt"),collect_set("city").alias("names")).orderBy(col("cnt"))
#ndf=df.drop("email","state","zip","web","city")
#ndf=df
#ndf=df.withColumn("state",when(col("state")=="NY","NEWYORK").when(col("state")=="CA","CALIFORNIA").otherwise(col("state")))
#ndf=df.withColumn("state",when(col("state")=="NY","NewYork").when(col("state")=="CA","Cali").otherwise(col("state")))
#ndf=df.withColumnRenamed("first_name","fname").withColumnRenamed("last_name","lname")
#ndf=df.withColumn("address1",when(col("address").contains("#"),"****").otherwise(col("address")))
#ndf=df.withColumn("substr",substring("email",0,6))
#ndf=df.withColumn("username",substring_index("email","@",1)).withColumn("emailsadd",substring_index("email","@",-1))
#df.createOrReplaceTempView("tab")

#qry='''with temp as(select *,concat_ws('-',first_name,last_name) fullname,substring_index(email,'@',-1) username from tab)
# select username,count(*) cnt from temp group by username order by cnt desc'''
#ndf=spark.sql(qry)


ndf.show(truncate=False)
ndf.printSchema()