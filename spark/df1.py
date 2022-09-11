from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data = "D:\\BigData\\datasets\\10000Records.csv"
df=spark.read.csv(data,header=True,inferSchema=True)
num=df.count()
import configparser
from configparser import ConfigParser
conf=configparser()
conf.read(r"D:\\BigData\\datasets\\config.txt")
host="jdbc:mysql://mysql.cempengo6sg1.ap-south-1.rds.amazonaws.com:3306/mysql_db?UseSSL=False"
uname="myuser"
pwd="mypassword"
import re
cols=[re.sub('[^a-zA-Z0-9]',"",c.lower()) for c in df.columns]
ndf=df.toDF(*cols)
ndf.write.mode("overwrite").format("jdbc").option("url","host").option("user","uname").option("password","pwd")\
    .option("driver","com.mysql.jdbc.Driver").option("dbtable","record1").save()
#df.show(20,truncate=False)
#df.printSchema()
#ndf.show(20,truncate=False)
#ndf.printSchema()
#ndf.write.mode("overwrite").format("jdbc").option("dbtable","emple1").option("user","")
res=ndf.groupby("gender").agg(count(col("*")).alias("cnt"))
res.show()
res.printSchema()