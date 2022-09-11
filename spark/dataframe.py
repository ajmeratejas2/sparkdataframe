from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data = "D:\\BigData\\datasets\\bank-full.csv"
odata=spark.read.csv(data,header=True,sep=";",inferSchema=True)
import configparser
from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"D:\\BigData\\datasets\\config.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")
#res=odata.where(col("age")>30)
#res= odata.select(col("age"),col("job") ).where((col("job")=="entrepreneur") & (col("balance")>1000))
#res=odata.groupby("marital").sum(("balance"))
res=odata.groupby("marital").agg(count("*").alias("cnt"),sum("balance").alias("smb")).orderBy(col("marital").desc())
#res.write.mode("overwrite").format("jdbc").option("dbtable","bankf").option("url",host).option("user",user).option("password",pwd)\
 #   .option("driver","com.mysql.cj.jdbc.Driver").save()
res.show()
#odata.printSchema()

