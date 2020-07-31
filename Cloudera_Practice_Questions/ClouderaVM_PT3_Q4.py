'''

sqoop import --connect "jdbc:mysql://localhost/retail_db" --password cloudera --username root --table orders --target-dir /user/cloudera/problem4_ques6/input --as-parquetfile

orders.write.format("parquet").save("/user/gmadhu89/PT3/Q4/Input")
Input file is provided at below HDFS Location.

/user/gmadhu89/PT3/Q4/Input
Save the data to hdfs using no compression as sequence file.

Result should be saved in at /user/cloudera/problem4_ques6/output
fields should be seperated by pipe delimiter.
Key should be order_id, value should be all fields seperated by a pipe.

'''
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Practice Test Question 3").getOrCreate()
sc = spark.sparkContext

from pyspark.sql.types import IntegerType,FloatType
from pyspark.sql.functions import col

orders = spark.read.format("parquet").load("/user/gmadhu89/PT3/Q4/Input")

ordersRDD = orders.rdd.map(lambda rec : (int(rec['_c0']) , str(rec['_c0'])+'|'+str(rec['_c1'])+'|'+str(rec['_c2'])+'|'+str(rec['_c3'])))
ordersRDD.saveAsSequenceFile("/user/gmadhu89/PT3/Q4/Output1",compressionCodecClass=None)
#orders.write.format("sequence").mode("overwrite").option("compression","None").save("/user/gmadhu89/PT3/Q4/Output")