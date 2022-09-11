from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").config("spark.sql.session.timeZone", "EST").getOrCreate()
data="D:\\BigData\\datasets\\donations.csv"
df=spark.read.csv(data,header=True,inferSchema=True)
def num(days):
    yr= int(days/365)
    mn=int((days%365)/30)
    day=int((days%365)%30)
    full=f"%yr years %mn months %day days"
#ndf=df.withColumn('dt',to_date(col("dt"),"d-M-yyyy")).withColumn("today",current_date())\
#   .withColumn("days",datediff(col("today"),col("dt"))).withColumn("tm",current_timestamp()).withColumn("dsr",date_add(col("today"),25))
ndf=df.withColumn("today",current_date())\
    .withColumn("dt",to_date(col("dt"),"d-M-yyyy"))\
    .withColumn("fday",next_day("today","Fri"))\
    .withColumn("lastdt",last_day(col("dt")))\
    .withColumn("dtformat",date_format(col("dt"),"dd-MM-yyyy-EEE"))\
    .withColumn("monlstSun",next_day(date_add(last_day(col("dt")),-7),"Sun"))\
    .withColumn("daywek",dayofweek(col("dt")))\
    .withColumn("daymnt",dayofmonth(col("dt")))\
    .withColumn("dayyr",dayofyear(col("dt")))\
    .withColumn("datediff",datediff(col("today"),col("dt")))
ndf.show(truncate=False)
ndf.printSchema()
