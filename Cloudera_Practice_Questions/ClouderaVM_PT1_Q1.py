'''
Convert snappy compressed avro data-files stored at hdfs location /user/cloudera/practice1/question3  into parquet file.

orders = spark.read.csv("/public/retail_db/orders")
orders.write.format("com.databricks.spark.avro").option("compression","snappy").save("/user/gmadhu89/PT1/Q1/Input")

'''


#pyspark --packages com.databricks:spark-avro_2.11:4.0.0
pyspark --packages org.apache.spark:spark-avro_2.11:2.4.4


avroFile = spark.read.format("avro").load("hdfs://quickstart.cloudera:8020/user/cloudera/practice1/question3")

avroFile.select('order_id','order_status').write.option("compression","gzip").parquet("hdfs://quickstart.cloudera:8020/user/cloudera/practice1/question3/output/")