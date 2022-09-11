from pyspark.sql import *
from pyspark.sql.functions import *


spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
host="jdbc:mysql://mysql.cempengo6sg1.ap-south-1.rds.amazonaws.com:3306/mysql_db?UseSSL=False"
uname="myuser"
pwd="mypassword"
df=spark.read.format("jdbc").option("url",host)\
    .option("dbtable","EMP")\
    .option("user",uname).option("password",pwd)\
    .option("driver","com.mysql.jdbc.Driver").load()
res=df
res.write.mode("overwrite").format("jdbc").option("url",host)\
    .option("dbtable","empcleante")\
    .option("user",uname).option("password",pwd)\
   .option("driver","com.mysql.jdbc.Driver").save("s3://tejas2026/ram/demo")

res.show()
#df.printSchema()
