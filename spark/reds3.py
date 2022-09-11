from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
host="jdbc:redshift://redshift.cajv4teuilcw.ap-south-1.redshift.amazonaws.com:5439/dev"
df=spark.read.format("jdbc").option("url",host).option("user","ruser").option("password","Rpassword.1").option("driver","com.amazon.redshift.jdbc42.Driver").option("dbtable","sales").option("header","True").load()

#res=df.select([count(when(col(i).isNull(),i)).alias(i) for i in df.columns])
#res.show()
ndf=df.where(col("commission")>500)
ndf.write.mode("append").format("jdbc").option("url",host).option("user","ruser").option("password","Rpassword.1").option("driver","com.amazon.redshift.jdbc42.Driver").option("dbtable","sales2").option("header","True").save()

ndf.show()