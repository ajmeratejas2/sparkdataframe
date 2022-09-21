from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.streaming import *


spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc=spark.sparkContext
ssc= StreamingContext(sc, 10)
host="ec2-13-233-87-227.ap-south-1.compute.amazonaws.com"
lines = ssc.socketTextStream(host, 3213)
#lines.pprint()
def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w:w.split(","))

        df=rowRdd.toDF(["name","age","city"])
        hostt="jdbc:mysql://mysql.cempengo6sg1.ap-south-1.rds.amazonaws.com:3306/mysql_db?useSSL=false"
        ndf=df.where(col("city")=="hyd")
        ndf.write.mode("append").format("jdbc").option("url",hostt).option("user","myuser").option("password","mypassword") \
        .option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","sparkstreaming").save()
        df.show()
        ndf.show()
    except:
        pass

lines.foreachRDD(process)



ssc.start()
ssc.awaitTermination()
