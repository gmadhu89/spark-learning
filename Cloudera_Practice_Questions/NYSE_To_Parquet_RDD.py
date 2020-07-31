'''
Data is available in local file system under /data/nyse (ls -ltr /data/nyse)
Fields (stockticker:string, transactiondate:string, openprice:float, highprice:float, lowprice:float, closeprice:float, volume:bigint)
Convert file format to parquet
Save it /user/<YOUR_USER_ID>/nyse_parquet.py


spark2-submit --master yarn --conf spark.ui.port=12909 \
--executor-cores 2 \
--num-executors 6 \
--executor-memory 3G \
/home/gmadhu89/nyse_parquet.py

'''
#hadoop fs -copyFromLocal /data/nyse /user/gmadhu89/

from pyspark import SparkConf,SparkContext
conf=SparkConf().setAppName("Files to Parquet SC")
sc=SparkContext(conf=conf)
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Files to Parquet").getOrCreate()
from pyspark.sql import SQLContext
from pyspark import sql
sqlcontext=sql.SQLContext(sc)

from pyspark.sql import Row

nyse=sc.textFile("/user/gmadhu89/nyse/") \
.map(lambda rec: Row(stockticker=str(rec.split(",")[0]), transactiondate=str(rec.split(",")[1]), openprice=float(rec.split(",")[2]), \
highprice=float(rec.split(",")[3]),lowprice=float(rec.split(",")[4]),closeprice=float(rec.split(",")[5]),volume=int(rec.split(",")[6])  )) \
.toDF()

nyse.coalesce(4).write.parquet("/user/gmadhu89/nyse_parquet/",'overwrite')