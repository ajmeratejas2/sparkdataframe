from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
import configparser
from configparser import ConfigParser
conf=ConfigParser()
conf.read(r"D:\\BigData\\datasets\\config.txt")
host=conf.get("cred","host")
user=conf.get("cred","user")
pwd=conf.get("cred","pass")
#host=conf.get("cred","host")
#user=conf.get("cred","uname")
#password=conf.get("cred","pwd")
hostt="jdbc:mysql://mysql.cempengo6sg1.ap-south-1.rds.amazonaws.com:3306/mysql_db?useSSL=false"
uname="myuser"
pwd1="mypassword"
df=spark.read.format("jdbc").option("url",hostt).option("user",uname).option("password",pwd).option("driver","com.mysql.jdbc.Driver")\
    .option("dbtable","EMP").load()
ndf=df
ndf.write.mode("overwrite").format("jdbc").option("url",host).option("user",user).option("password",pwd)\
    .option("driver","com.mysql.cj.jdbc.Driver")\
    .option("dbtable","free").save()
#ndf.na.fill(0).show()
#df.show()
