'''

Provided customer tab delimited files at below HDFS location.
Input folder is  /user/cloudera/practice1/problem6
Find all customers that lives 'Caguas' city.


Output Requirement:
Result should be saved in /user/cloudera/practice1/problem6/output
Output file should be saved in avro format in deflate compression.


Source : /user/gmadhu89/customer_test_data/part-00000-363bd16c-df1d-4aae-a342-db9f2876eb1b-c000.csv

'''
## --packages com.databricks:spark-avro_2.10:2.0.1
##--packages com.databricks:spark-avro_2.11:4.0.0  --For Spark 2
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Read Tab delimited file").getOrCreate()
sc = spark.sparkContext

customers = spark.read.format("csv").option("header","true").option("sep","\t").load("/user/gmadhu89/customer_test_data/part-00000-363bd16c-df1d-4aae-a342-db9f2876eb1b-c000.csv")

customers_filter = customers.filter(customers.customer_city == 'Caguas')

customers_filter.write.format("com.databricks.spark.avro").option("compression","deflate").save("/user/gmadhu89/customer_test_data/output/")